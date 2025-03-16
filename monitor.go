package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

func main() {
	// Define command line flags
	logFilePath := flag.String("file", "", "Path to HDFS log file to monitor")
	checkInterval := flag.Int("interval", 5, "Check interval in seconds")
	flag.Parse()

	// Validate log file path
	if *logFilePath == "" {
		log.Fatal("Please provide a log file path using the -file flag")
	}

	// Resolve to absolute path
	absPath, err := filepath.Abs(*logFilePath)
	if err != nil {
		log.Fatalf("Failed to resolve absolute path: %v", err)
	}

	// Check if file exists
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		log.Fatalf("Log file does not exist: %s", absPath)
	}

	fmt.Printf("Starting to monitor HDFS log file: %s\n", absPath)
	fmt.Printf("Checking for updates every %d seconds\n", *checkInterval)

	// Get initial file info
	lastSize, lastModTime := getFileInfo(absPath)
	fmt.Printf("Initial file size: %d bytes, last modified: %s\n", lastSize, lastModTime)

	// Monitor loop
	ticker := time.NewTicker(time.Duration(*checkInterval) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		currentSize, currentModTime := getFileInfo(absPath)

		if currentSize > lastSize {
			bytesAdded := currentSize - lastSize
			fmt.Printf("[%s] File updated: %d new bytes added (total size: %d bytes)\n",
				time.Now().Format("2006-01-02 15:04:05"), bytesAdded, currentSize)

			// Optional: Read and display the new content
			displayNewContent(absPath, lastSize, currentSize)

			lastSize = currentSize
			lastModTime = currentModTime
		} else if currentModTime.After(lastModTime) {
			fmt.Printf("[%s] File modified but size unchanged (size: %d bytes)\n",
				time.Now().Format("2006-01-02 15:04:05"), currentSize)
			lastModTime = currentModTime
		}
	}
}

// getFileInfo returns the size and modification time of a file
func getFileInfo(filePath string) (int64, time.Time) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		log.Printf("Warning: Failed to get file info: %v", err)
		return 0, time.Time{}
	}
	return fileInfo.Size(), fileInfo.ModTime()
}

// displayNewContent reads and displays new content added to the file
func displayNewContent(filePath string, oldSize, newSize int64) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Warning: Failed to open file: %v", err)
		return
	}
	defer file.Close()

	// Seek to the position of the old file size
	_, err = file.Seek(oldSize, 0)
	if err != nil {
		log.Printf("Warning: Failed to seek in file: %v", err)
		return
	}

	// Read the new content
	newContent := make([]byte, newSize-oldSize)
	_, err = file.Read(newContent)
	if err != nil {
		log.Printf("Warning: Failed to read new content: %v", err)
		return
	}

	fmt.Println("--- New content ---")
	fmt.Println(string(newContent))
	fmt.Println("------------------")
}