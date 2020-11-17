package main

import (
	"bytes"
	//	"encoding/hex"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glacier"
	"io"
	"os"
	"strconv"
	"sync"
	//        "runtime"
)

const (
	//	DEFAULT_CHUNKSIZE = "1073741824" // 1 Gib
	//	DEFAULT_CHUNKSIZE = "134217728" // 128 Mib
	//	DEFAULT_CHUNKSIZE = "33554432" // 32 Mib
	DEFAULT_CHUNKSIZE = "16777216" // 16 Mib
	//	DEFAULT_CHUNKSIZE = "1048576" // 1 Mib
	DEFAULT_REGION = "eu-central-1"
)

type PartUpload struct { //n (Length) bytes (Body) starting at Position in file
	Body     []byte
	Checksum []byte
	Position int64
	Length   int
}

type Parameters struct {
	VaultName   string
	ChunkSize   int64
	Description string
	Archive     string
	File        *os.File
	FileInfo    os.FileInfo
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

//var m runtime.MemStats

func main() {
	vaultPtr := flag.String("vaultname", "", "The vault name (MANDATORY)")
	chunkSizePtr := flag.String("chunksize", DEFAULT_CHUNKSIZE, "The chunk size (default 16 MiB). Power of 2")
	descriptionsPtr := flag.String("description", "", "The archive description")
	archivePtr := flag.String("archive", "", "The archive name (MANDATORY)")
	regionPtr := flag.String("region", DEFAULT_REGION, "The AWS region (default eu-central-1)")

	flag.Parse()

	if *vaultPtr == "" || *archivePtr == "" {
		flag.PrintDefaults()
		panic("Bad Arguments")
	}

	chunkSize, err := strconv.ParseInt(*chunkSizePtr, 10, 64)
	check(err)

	file, err := os.Open(*archivePtr)
	if os.IsNotExist(err) {
		panic("The file does not exists")
	}
	check(err)
	fi, err := os.Stat(*archivePtr)
	check(err)

	if int64(chunkSize) >= fi.Size() {
		chunkSize = fi.Size()
	}

	params := Parameters{
		*vaultPtr,
		chunkSize,
		*descriptionsPtr,
		*archivePtr,
		file,
		fi,
	}

	sess, err := session.NewSession()
	if err != nil {
		panic("No AWS session.")
	}

	svc := glacier.New(sess, &aws.Config{
		Region: aws.String(*regionPtr),
	})

	var bufpool = make(chan []byte, 3)

	var wg sync.WaitGroup
	wg.Add(3)
	chPartsLoad := make(chan PartUpload, 3)
	chPartsReady := make(chan PartUpload, 10)
	chChecksum := make(chan string)
	var binaryCheckSums [][]byte
	imuOut := initiateMultipartUpload(&params, svc)
	fmt.Println("Transfer is starting")
	go loadPartsFromDisk(&params, chPartsLoad, bufpool, &wg)
	go uploadPrecompute(chPartsLoad, chPartsReady, binaryCheckSums, chChecksum, &wg)
	go uploadParts(&params, svc, imuOut, chPartsReady, bufpool, &wg)
	treeChecksum := <-chChecksum
	wg.Wait()
	completeMultipartUpload(&params, svc, treeChecksum, imuOut)
	fmt.Println("Transfer is done")
}

func getBuffer(pool chan []byte, chunkSize int64) []byte {
	var b []byte
	select {
	case b = <-pool:
	default:
		b = make([]byte, chunkSize)
	}
	return b
}

func releaseBuffer(pool chan []byte, buf []byte) {
	select {
	case pool <- buf:
	default:

	}
}

func initiateMultipartUpload(params *Parameters, svc *glacier.Glacier) *glacier.InitiateMultipartUploadOutput {
	imu := glacier.InitiateMultipartUploadInput{
		//	AccountId:          aws.String("foo"),
		ArchiveDescription: aws.String(params.Description),
		PartSize:           aws.String(strconv.FormatInt(params.ChunkSize, 10)),
		VaultName:          aws.String(params.VaultName),
	}
	imuOut, err := svc.InitiateMultipartUpload(&imu)
	check(err)

	return imuOut
}

func loadPartsFromDisk(params *Parameters, ch chan PartUpload, pool chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()
	defer params.File.Close()

	var position int64 = 0
	for {
		//		buf := make([]byte, params.ChunkSize)
		buf := getBuffer(pool, params.ChunkSize)
		bytesRead, err := params.File.Read(buf)
		//fmt.Printf("D- %v bytes read\n", bytesRead)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
			}
			//fmt.Println("D - EOF encoutered")
			close(ch)
			break
		}
		part := PartUpload{
			buf[:bytesRead],
			nil,
			position,
			bytesRead,
		}
		ch <- part
		//fmt.Printf("D - Loaded Bloc %v\n", position)
		position += int64(bytesRead)
	}
}

func uploadPrecompute(chBefore chan PartUpload, chCompute chan PartUpload, binaryCheckSums [][]byte, chChecksum chan string, wg *sync.WaitGroup) string {
	defer wg.Done()
	for part := range chBefore {
		b := bytes.NewReader(part.Body)
		hh := glacier.ComputeHashes(b)
		//fmt.Printf("D- HH value %x\n", hh.TreeHash)
		tmpHash := hh.TreeHash
		//hexHash := hex.EncodeToString(tmpHash)
		binaryCheckSums = append(binaryCheckSums, tmpHash[:])
		part.Checksum = tmpHash[:]
		//fmt.Printf("D- Bloc %v, has hash %x\n", part.Position, string(part.Checksum))
		chCompute <- part
	}
	close(chCompute)
	checksum := fmt.Sprintf("%x", string(glacier.ComputeTreeHash(binaryCheckSums)))
	chChecksum <- checksum
	close(chChecksum)
	return checksum
}

func uploadParts(params *Parameters, svc *glacier.Glacier, imout *glacier.InitiateMultipartUploadOutput, ch chan PartUpload, pool chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()
	for part := range ch {
		partRange := fmt.Sprintf("bytes %v-%v/*", part.Position, part.Position+int64(part.Length-1))
		fmt.Println(partRange)
		rseek := bytes.NewReader(part.Body)
		umpi := glacier.UploadMultipartPartInput{
			//	AccountId: aws.String("foo"),
			Body:      rseek,
			Checksum:  aws.String(fmt.Sprintf("%x", part.Checksum)),
			Range:     aws.String(partRange),
			UploadId:  imout.UploadId,
			VaultName: aws.String(params.VaultName),
		}

		res, _ /*resp*/ := svc.UploadMultipartPartRequest(&umpi)
		err := res.Send()
		check(err)
		releaseBuffer(pool, part.Body)
		//runtime.ReadMemStats(&m)
		//        fmt.Printf("%d,%d,%d,%d\n", m.HeapSys, m.HeapAlloc,
		//          m.HeapIdle, m.HeapReleased)
		/*
			if err == nil {
				fmt.Println(resp)
			} else {
				fmt.Printf("Send error(%x): %v", string(part.Checksum), err)
			}*/
		//fmt.Println(part.Position) //DEBUG
	}
	fmt.Println("Upload is done")
}

func completeMultipartUpload(params *Parameters, svc *glacier.Glacier, treeChecksum string, imout *glacier.InitiateMultipartUploadOutput) {
	fmt.Println("Completing upload")
	fiSize := params.FileInfo.Size()
	cmui := glacier.CompleteMultipartUploadInput{
		//	AccountId:   aws.String("foo"),
		ArchiveSize: aws.String(strconv.FormatInt(fiSize, 10)),
		Checksum:    aws.String(treeChecksum),
		UploadId:    imout.UploadId,
		VaultName:   aws.String(params.VaultName),
	}
	arch, err := svc.CompleteMultipartUpload(&cmui)
	check(err)
	fmt.Printf("Transfer ok:\n\n")
	fmt.Println(arch)
	fmt.Printf("\n\n")
	/*
		if err != nil {
			fmt.Println("error")
			fmt.Println(err)
		} else {
			fmt.Println(arch)
		}*/
}
