# Bulk Zip File Processor

## Overview
A Python utility for batch unzipping files, organizing extracted files by folder name, and distributing them into subfolders with a specified file limit.

## Features
- Extracts zip files into directories named after the zip file.
- Organizes extracted files into subfolders (`batch_1`, `batch_2`, etc.).
- Supports parallel processing for efficient execution.

## Requirements
- Python 3.6+
- Libraries: `os`, `zipfile`, `shutil`, `concurrent.futures`

## Installation
Clone the repository:
```bash
git clone https://github.com/yourusername/bulk-zip-file-processor.git
cd bulk-zip-file-processor
