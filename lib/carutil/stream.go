package carutil

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	carstorage "github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

// CreateCarFromReader 从 io.Reader 创建 CAR 文件
// 这个实现使用临时文件，因为 blockstore.OpenReadWrite 需要文件路径
func CreateCarFromReader(ctx context.Context, reader io.Reader, fileName string, carWriter io.Writer) (cid.Cid, error) {
	// 读取所有数据
	data, err := io.ReadAll(reader)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to read data: %w", err)
	}

	// 创建 CID
	fileCid, err := createCIDFromData(data)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to create CID: %w", err)
	}

	// 创建临时文件
	tempFile, err := os.CreateTemp("", "car-*.tmp")
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tempFile.Name()) // 清理临时文件
	defer tempFile.Close()

	// 使用 blockstore 写入 CAR 数据
	writeStore, err := blockstore.OpenReadWrite(tempFile.Name(), []cid.Cid{fileCid})
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to create writable blockstore: %w", err)
	}

	// 创建块并写入
	block, err := blocks.NewBlockWithCid(data, fileCid)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to create block: %w", err)
	}

	// 写入块到存储
	if err := writeStore.Put(ctx, block); err != nil {
		return cid.Cid{}, fmt.Errorf("failed to put block: %w", err)
	}

	// 完成写入
	if err := writeStore.Finalize(); err != nil {
		return cid.Cid{}, fmt.Errorf("failed to finalize blockstore: %w", err)
	}

	// 将临时文件内容复制到目标写入器
	tempFile.Seek(0, 0) // 重置到文件开头
	if _, err := io.Copy(carWriter, tempFile); err != nil {
		return cid.Cid{}, fmt.Errorf("failed to copy CAR data: %w", err)
	}

	return fileCid, nil
}

// CreateCarFromReaderWithPath 直接写入文件路径的版本（推荐）
func CreateCarFromReaderWithPath(ctx context.Context, reader io.Reader, fileName string, carFilePath string) (cid.Cid, error) {
	// 读取所有数据
	data, err := io.ReadAll(reader)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to read data: %w", err)
	}

	// 创建 CID
	fileCid, err := createCIDFromData(data)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to create CID: %w", err)
	}

	// 使用 blockstore 直接写入目标文件
	writeStore, err := blockstore.OpenReadWrite(carFilePath, []cid.Cid{fileCid})
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to create writable blockstore: %w", err)
	}

	// 创建块并写入
	block, err := blocks.NewBlockWithCid(data, fileCid)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to create block: %w", err)
	}

	// 写入块到存储
	if err := writeStore.Put(ctx, block); err != nil {
		return cid.Cid{}, fmt.Errorf("failed to put block: %w", err)
	}

	// 完成写入
	if err := writeStore.Finalize(); err != nil {
		return cid.Cid{}, fmt.Errorf("failed to finalize blockstore: %w", err)
	}

	return fileCid, nil
}

// CreateCarFromReaderChunked 分块版本，适用于大文件
func CreateCarFromReaderChunked(ctx context.Context, reader io.Reader, fileName string, carFilePath string, chunkSize int) (cid.Cid, error) {
	if chunkSize <= 0 {
		chunkSize = 1024 * 1024 // 默认 1MB
	}

	var blockCids []cid.Cid
	var allBlocks []blocks.Block

	// 分块读取数据
	for {
		chunk := make([]byte, chunkSize)
		n, err := reader.Read(chunk)
		if err == io.EOF {
			break
		}
		if err != nil {
			return cid.Cid{}, fmt.Errorf("failed to read chunk: %w", err)
		}

		if n > 0 {
			chunkData := chunk[:n]
			chunkCid, err := createCIDFromData(chunkData)
			if err != nil {
				return cid.Cid{}, fmt.Errorf("failed to create CID for chunk: %w", err)
			}

			block, err := blocks.NewBlockWithCid(chunkData, chunkCid)
			if err != nil {
				return cid.Cid{}, fmt.Errorf("failed to create block: %w", err)
			}

			blockCids = append(blockCids, chunkCid)
			allBlocks = append(allBlocks, block)
		}
	}

	if len(allBlocks) == 0 {
		return cid.Cid{}, fmt.Errorf("no data to process")
	}

	// 如果只有一个块，直接使用该块作为根
	if len(allBlocks) == 1 {
		writeStore, err := blockstore.OpenReadWrite(carFilePath, []cid.Cid{blockCids[0]})
		if err != nil {
			return cid.Cid{}, err
		}

		if err := writeStore.Put(ctx, allBlocks[0]); err != nil {
			return cid.Cid{}, err
		}

		if err := writeStore.Finalize(); err != nil {
			return cid.Cid{}, err
		}

		return blockCids[0], nil
	}

	// 多个块：创建根节点
	rootCid, rootData, err := createRootNode(blockCids)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to create root node: %w", err)
	}

	// 准备所有根 CID
	allRoots := []cid.Cid{rootCid}

	// 创建 blockstore
	writeStore, err := blockstore.OpenReadWrite(carFilePath, allRoots)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to create writable blockstore: %w", err)
	}

	// 写入根节点
	rootBlock, err := blocks.NewBlockWithCid(rootData, rootCid)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to create root block: %w", err)
	}
	if err := writeStore.Put(ctx, rootBlock); err != nil {
		return cid.Cid{}, fmt.Errorf("failed to put root block: %w", err)
	}

	// 写入所有数据块
	for _, block := range allBlocks {
		if err := writeStore.Put(ctx, block); err != nil {
			return cid.Cid{}, fmt.Errorf("failed to put block %s: %w", block.Cid(), err)
		}
	}

	// 完成写入
	if err := writeStore.Finalize(); err != nil {
		return cid.Cid{}, fmt.Errorf("failed to finalize blockstore: %w", err)
	}

	return rootCid, nil
}

// CreateCarFromBytes 从字节数组创建 CAR 文件
func CreateCarFromBytes(ctx context.Context, data []byte, fileName string, carFilePath string) (cid.Cid, error) {
	// 创建 CID
	fileCid, err := createCIDFromData(data)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to create CID: %w", err)
	}

	// 创建 blockstore
	writeStore, err := blockstore.OpenReadWrite(carFilePath, []cid.Cid{fileCid})
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to create writable blockstore: %w", err)
	}

	// 创建并写入块
	block, err := blocks.NewBlockWithCid(data, fileCid)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to create block: %w", err)
	}

	if err := writeStore.Put(ctx, block); err != nil {
		return cid.Cid{}, fmt.Errorf("failed to put block: %w", err)
	}

	// 完成写入
	if err := writeStore.Finalize(); err != nil {
		return cid.Cid{}, fmt.Errorf("failed to finalize blockstore: %w", err)
	}

	return fileCid, nil
}

// createCIDFromData 从数据创建 CID
func createCIDFromData(data []byte) (cid.Cid, error) {
	hasher := sha256.New()
	hasher.Write(data)
	hash := hasher.Sum(nil)

	mh, err := multihash.Encode(hash, multihash.SHA2_256)
	if err != nil {
		return cid.Cid{}, err
	}

	return cid.NewCidV1(cid.Raw, mh), nil
}

// createRootNode 创建包含所有块引用的根节点
func createRootNode(blockCids []cid.Cid) (cid.Cid, []byte, error) {
	// 创建简单的索引数据
	var cidStrings []string
	for _, c := range blockCids {
		cidStrings = append(cidStrings, c.String())
	}

	rootData := fmt.Sprintf(`{"blocks":["%s"],"type":"file_chunks"}`,
		joinStrings(cidStrings, `","`))

	rootCid, err := createCIDFromData([]byte(rootData))
	if err != nil {
		return cid.Cid{}, nil, err
	}

	return rootCid, []byte(rootData), nil
}

// joinStrings 字符串连接函数
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	if len(strs) == 1 {
		return strs[0]
	}

	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}

// ReadCarFile 读取 CAR 文件
func ReadCarFile(carFilePath string) ([]cid.Cid, map[cid.Cid][]byte, error) {
	// 打开只读 blockstore
	readStore, err := blockstore.OpenReadOnly(carFilePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open readonly blockstore: %w", err)
	}
	defer readStore.Close()

	// 获取根 CID
	roots, err := readStore.Roots()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get roots: %w", err)
	}

	// 读取所有块
	blocks := make(map[cid.Cid][]byte)
	ctx := context.Background()

	// 简单实现：只读取根块
	for _, root := range roots {
		block, err := readStore.Get(ctx, root)
		if err != nil {
			continue // 跳过读取失败的块
		}
		blocks[root] = block.RawData()
	}

	return roots, blocks, nil
}

// ExtractDataFromCar 从 CAR 文件中提取特定 CID 的数据
func ExtractDataFromCar(carFilePath string, targetCid cid.Cid) ([]byte, error) {
	readStore, err := blockstore.OpenReadOnly(carFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open readonly blockstore: %w", err)
	}
	defer readStore.Close()

	ctx := context.Background()
	block, err := readStore.Get(ctx, targetCid)
	if err != nil {
		return nil, fmt.Errorf("failed to get block %s: %w", targetCid.String(), err)
	}

	return block.RawData(), nil
}

// HelperForHttpHandler 为 HTTP 处理函数提供的便利方法
// 由于你的场景中 carFile 是一个已经打开的文件，这个方法直接操作该文件
func HelperForHttpHandler(ctx context.Context, reader io.Reader, fileName string, carFile *os.File) (cid.Cid, error) {
	// 获取文件路径
	carPath := carFile.Name()

	// 关闭原有文件句柄，让 blockstore 管理文件
	carFile.Close()

	// 使用路径版本的函数
	return CreateCarFromReaderWithPath(ctx, reader, fileName, carPath)
}

// WriteReaderToCar 将 io.Reader 的数据写入到 CAR 存储中
func WriteReaderToCar(ctx context.Context, reader io.Reader, wCar carstorage.WritableCar) (cid.Cid, error) {
	sCar := wCar.(*carstorage.StorageCar)
	ls := cidlink.DefaultLinkSystem()
	ls.TrustedStorage = true

	ls.StorageReadOpener = func(_ ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		cl, ok := l.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("not a cidlink")
		}
		blk, err := sCar.Get(ctx, cl.Cid.KeyString())
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(blk), nil
	}

	ls.StorageWriteOpener = func(_ ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(l ipld.Link) error {
			cl, ok := l.(cidlink.Link)
			if !ok {
				return fmt.Errorf("not a cidlink")
			}
			blk, err := blocks.NewBlockWithCid(buf.Bytes(), cl.Cid)
			if err != nil {
				return err
			}
			sCar.Put(ctx, blk.Cid().KeyString(), blk.RawData())
			return nil
		}, nil
	}

	// 从 reader 构建 UnixFS 文件
	link, _, err := builder.BuildUnixFSFile(reader, "", &ls)
	if err != nil {
		return cid.Cid{}, err
	}

	root := link.(cidlink.Link)
	return root.Cid, nil
}

// WriteReaderToFile 将 io.Reader 的数据写入到指定的 CAR 文件
func WriteReaderToFile(ctx context.Context, reader io.Reader, outputPath string) (cid.Cid, error) {
	// 创建临时的 CID 作为占位符
	hasher, err := multihash.GetHasher(multihash.SHA2_256)
	if err != nil {
		return cid.Cid{}, err
	}
	digest := hasher.Sum([]byte{})
	hash, err := multihash.Encode(digest, multihash.SHA2_256)
	if err != nil {
		return cid.Cid{}, err
	}
	proxyRoot := cid.NewCidV1(uint64(multicodec.DagPb), hash)

	// 打开可读写的 blockstore
	cdest, err := blockstore.OpenReadWrite(outputPath, []cid.Cid{proxyRoot})
	if err != nil {
		return cid.Cid{}, err
	}
	defer cdest.Finalize()

	// 设置链接系统
	ls := cidlink.DefaultLinkSystem()
	ls.TrustedStorage = true

	ls.StorageReadOpener = func(_ ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		cl, ok := l.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("not a cidlink")
		}
		blk, err := cdest.Get(ctx, cl.Cid)
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(blk.RawData()), nil
	}

	ls.StorageWriteOpener = func(_ ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(l ipld.Link) error {
			cl, ok := l.(cidlink.Link)
			if !ok {
				return fmt.Errorf("not a cidlink")
			}
			blk, err := blocks.NewBlockWithCid(buf.Bytes(), cl.Cid)
			if err != nil {
				return err
			}
			return cdest.Put(ctx, blk)
		}, nil
	}

	// 从 reader 构建 UnixFS 文件
	link, _, err := builder.BuildUnixFSFile(reader, "", &ls)
	if err != nil {
		return cid.Cid{}, err
	}

	root := link.(cidlink.Link)

	// 完成写入并替换根 CID
	if err := cdest.Finalize(); err != nil {
		return cid.Cid{}, err
	}

	// 替换文件中的根 CID
	err = car.ReplaceRootsInFile(outputPath, []cid.Cid{root.Cid})
	if err != nil {
		return cid.Cid{}, err
	}

	return root.Cid, nil
}
