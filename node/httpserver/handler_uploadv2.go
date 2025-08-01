package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/carutil"
	fscrypto "github.com/Filecoin-Titan/titan/node/httpserver/crypto"

	"github.com/gbrlsnchs/jwt/v3"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
)

func (hs *HttpServer) uploadv2Handler(w http.ResponseWriter, r *http.Request) {
	log.Debug("uploadv2Handler")
	setAccessControlAllowForHeader(w)
	if r.Method == http.MethodOptions {
		return
	}

	if r.Method != http.MethodPost {
		uploadResult(w, -1, fmt.Sprintf("only allow post method, http status code %d", http.StatusMethodNotAllowed))
		return
	}

	userID, pass, err := verifyToken(r, hs.apiSecret)
	if err != nil {
		log.Errorf("verfiy token error: %s", err.Error())
		uploadResult(w, -1, fmt.Sprintf("%s, http status code %d", err.Error(), http.StatusUnauthorized))
		return
	}

	log.Infof("user %s upload file", userID)

	// limit max concurrent
	semaphore <- struct{}{}
	defer func() { <-semaphore }()

	// limit size
	r.Body = http.MaxBytesReader(w, r.Body, int64(hs.maxSizeOfUploadFile))

	var statusCode int
	var root cid.Cid
	contentType := getContentType(r)
	switch contentType {
	case "multipart/form-data":
		root, statusCode, err = hs.handleUploadFileV2(r, pass)
	default:
		log.Errorf("unsupported Content-type %s", contentType)
		statusCode = http.StatusBadRequest
		err = fmt.Errorf("unsupported Content-type %s", contentType)
	}

	if err != nil {
		log.Debugw("upload file error", "error", err.Error())
		uploadResult(w, -1, fmt.Sprintf("%s, http status code %d", err.Error(), statusCode))
		return
	}

	type Result struct {
		Code int    `json:"code"`
		Err  int    `json:"err"`
		Msg  string `json:"msg"`
		Cid  string `json:"cid"`
	}

	ret := Result{Code: 0, Err: 0, Msg: "", Cid: root.String()}
	buf, err := json.Marshal(ret)
	if err != nil {
		log.Errorf("marshal error %s", err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	if _, err = w.Write(buf); err != nil {
		log.Errorf("write error %s", err.Error())
	}

}

func verifyToken(r *http.Request, apiSecret *jwt.HMACSHA) (string, string, error) {
	token := r.Header.Get("Authorization")
	if token == "" {
		token = r.FormValue("token")
		if token != "" {
			token = "Bearer " + token
		}
	}

	if token != "" {
		if !strings.HasPrefix(token, "Bearer ") {
			return "", "", fmt.Errorf("missing Bearer prefix in auth header")
		}
		token = strings.TrimPrefix(token, "Bearer ")
	}

	payload := &types.JWTPayload{}
	if _, err := jwt.Verify([]byte(token), apiSecret, payload); err != nil {
		return "", "", err
	}

	return payload.ID, payload.FilePassNonce, nil
}

func (hs *HttpServer) handleUploadFileV2(r *http.Request, passNonce string) (cid.Cid, int, error) {
	// Get the uploaded file
	mr, err := r.MultipartReader()
	if err != nil {
		return cid.Cid{}, http.StatusBadRequest, fmt.Errorf("invalid multipart request: %w", err)
	}

	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return cid.Cid{}, http.StatusBadRequest, err
		}
		if part.FormName() == "file" && part.FileName() != "" {
			result, status, err := hs.processFilePart(part, passNonce)
			part.Close()
			if err != nil {
				return cid.Cid{}, status, err
			}
			return result, status, nil
		}

		part.Close()
	}

	return cid.Cid{}, http.StatusBadRequest, errors.New("no file part found")
}

func (hs *HttpServer) processFilePart(part *multipart.Part, passNonce string) (cid.Cid, int, error) {
	var reader io.Reader = part
	fileName := part.FileName()

	// 获取有指定大小空间的配置路径
	assetDir, err := hs.asset.AllocatePathWithSize(0)
	if err != nil {
		log.Debugw("Allocate storage error", "error", err.Error())
		return cid.Cid{}, http.StatusInsufficientStorage, fmt.Errorf("can not allocate storage for file %s", err.Error())
	}
	// 创建临时car文件存储目录
	tempCarFile := path.Join(assetDir, uuid.NewString())
	defer func() {
		if removeErr := os.RemoveAll(tempCarFile); removeErr != nil {
			log.Debugw("Failed to cleanup temp file", "path", tempCarFile, "error", removeErr.Error())
		}
	}()

	// 判断文件是否需要加密，并加密文件
	if passNonce != "" {
		reader, err = fscrypto.EncryptStream(part, []byte(passNonce))
		if err != nil {
			return cid.Cid{}, http.StatusInternalServerError, err
		}
	}

	// 创建临时car文件存储目录
	rootCID, err := carutil.CreateCarFromReaderWithPath(context.Background(), reader, fileName, tempCarFile)
	if err != nil {
		log.Debugw("create car error", "error", err.Error())
		return cid.Cid{}, http.StatusInternalServerError, fmt.Errorf("create car failed: %s, path: %s", err.Error(), tempCarFile)
	}
	exists, err := hs.asset.AssetExists(rootCID)
	if err != nil {
		log.Debugw("check asset exist error", "error", err.Error())
		return cid.Cid{}, http.StatusInternalServerError, err
	}
	if exists {
		return rootCID, http.StatusOK, nil
	}

	if err = hs.saveCarFile(context.Background(), tempCarFile, rootCID); err != nil {
		return cid.Cid{}, http.StatusInternalServerError, fmt.Errorf("save car file failed %s, file %s", err.Error(), fileName)
	}
	return rootCID, http.StatusOK, nil
}

func (hs *HttpServer) saveCarFile(ctx context.Context, tempCarFile string, root cid.Cid) error {
	f, err := os.Open(tempCarFile)
	if err != nil {
		log.Debugw("open car file error", "error", err.Error())
		return err
	}
	defer f.Close()

	fInfo, err := f.Stat()
	if err != nil {
		log.DPanicw("get car file size error", "error", err.Error())
		return err
	}
	log.Debugf("car file size %d", fInfo.Size())
	if err := hs.asset.SaveUserAsset(ctx, uuid.NewString(), root, fInfo.Size(), f); err != nil {
		log.Debugw("save asset error", "error", err.Error())
		return err
	}
	return nil
}
