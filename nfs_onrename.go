package nfs

import (
	"bytes"
	"context"
	"os"
	"reflect"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

var doubleWccErrorBody = [16]byte{}

func onRename(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = errFormatterWithBody(doubleWccErrorBody[:])
	from := DirOpArg{}
	err := xdr.Read(w.req.Body, &from)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	fs, fromPath, err := userHandle.FromHandle(from.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}

	to := DirOpArg{}
	if err = xdr.Read(w.req.Body, &to); err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	fs2, toPath, err := userHandle.FromHandle(to.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	// check the two fs are the same
	if !reflect.DeepEqual(fs, fs2) {
		return &NFSStatusError{NFSStatusNotSupp, os.ErrPermission}
	}

	if !billy.CapabilityCheck(fs, billy.WriteCapability) {
		return &NFSStatusError{NFSStatusROFS, os.ErrPermission}
	}

	if len(string(from.Filename)) > PathNameMax || len(string(to.Filename)) > PathNameMax {
		return &NFSStatusError{NFSStatusNameTooLong, os.ErrInvalid}
	}

	fromDirPath := fs.Join(fromPath...)
	fromDirInfo, err := fs.Stat(fromDirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		return &NFSStatusError{NFSStatusIO, err}
	}
	if !fromDirInfo.IsDir() {
		return &NFSStatusError{NFSStatusNotDir, nil}
	}
	preCacheData := ToFileAttribute(fromDirInfo, fromDirPath).AsCache()

	toDirPath := fs.Join(toPath...)
	toDirInfo, err := fs.Stat(toDirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		return &NFSStatusError{NFSStatusIO, err}
	}
	if !toDirInfo.IsDir() {
		return &NFSStatusError{NFSStatusNotDir, nil}
	}
	preDestData := ToFileAttribute(toDirInfo, toDirPath).AsCache()

	oldPath := append(fromPath, string(from.Filename))
	newPath := append(toPath, string(to.Filename))

	fromLoc := fs.Join(oldPath...)
	toLoc := fs.Join(newPath...)

	err = fs.Rename(fromLoc, toLoc)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		if os.IsPermission(err) {
			return &NFSStatusError{NFSStatusAccess, err}
		}
		return &NFSStatusError{NFSStatusIO, err}
	}

	// Update all handles pointing to the old path to point to the new path.
	// This is critical for NFS silly rename support (unlink while file is open).
	// We use type assertion to check if the handler supports UpdateHandlesByPath,
	// which updates handles by path lookup rather than relying on ToHandle
	// (which may fail due to filesystem instance comparison issues).
	if updater, ok := userHandle.(interface {
		UpdateHandlesByPath(billy.Filesystem, []string, []string) int
	}); ok {
		updater.UpdateHandlesByPath(fs, oldPath, newPath)
	} else {
		// Fall back to the old approach for handlers that don't support UpdateHandlesByPath
		oldHandle := userHandle.ToHandle(fs, oldPath)
		if err := userHandle.UpdateHandle(fs, oldHandle, newPath); err != nil {
			_ = userHandle.InvalidateHandle(fs, oldHandle)
		}
	}

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := WriteWcc(writer, preCacheData, tryStat(fs, fromPath)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WriteWcc(writer, preDestData, tryStat(fs, toPath)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}
