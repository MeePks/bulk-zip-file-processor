import os
import sys
import zipfile
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from math import ceil

def unzip_file(zip_file, dest_dir):
    """
    Unzips a single file to the specified destination directory.
    """
    try:
        with zipfile.ZipFile(zip_file, 'r') as zf:
            zf.extractall(dest_dir)
        print(f"Extracted {zip_file} to {dest_dir}")
    except Exception as e:
        print(f"Error processing {zip_file}: {e}")

def unzip_files_concurrently(zip_folder, output_base_folder):
    """
    Unzips all zip files in the folder concurrently.
    """
    zip_folder = Path(zip_folder)
    if not zip_folder.exists():
        print(f"Error: Folder '{zip_folder}' does not exist.")
        return []

    extracted_folders = []
    with ThreadPoolExecutor() as executor:
        for zip_file in zip_folder.glob("*.zip"):
            dest_dir = output_base_folder / zip_file.stem
            extracted_folders.append(dest_dir)
            os.makedirs(dest_dir, exist_ok=True)
            executor.submit(unzip_file, zip_file, dest_dir)

    return extracted_folders

def split_files_into_directories(folder, files_per_dir):
    """
    Splits the files in a folder into subdirectories with a maximum of `files_per_dir` files each.
    """
    folder = Path(folder)
    if not folder.exists():
        print(f"Error: Folder '{folder}' does not exist.")
        return []

    all_files = sorted(folder.glob("*.*"))  # Get all files in the folder
    subdirectories = []

    if not all_files:
        print(f"No files found in folder '{folder}'.")
        return []

    num_subdirs = ceil(len(all_files) / files_per_dir)

    for i in range(num_subdirs):
        sub_dir = folder / f"2024-06-0{i + 1}"
        os.makedirs(sub_dir, exist_ok=True)
        subdirectories.append(sub_dir)

        # Move the files to the subdirectory
        batch_files = all_files[i * files_per_dir:(i + 1) * files_per_dir]
        for file in batch_files:
            file.rename(sub_dir / file.name)

    return subdirectories

def generate_batch_files(folder, database, server, ssis_deployed_server, map_path, file_pattern="*.txt"):
    """
    Generates batch files for each file in the specified folder.
    """
    folder = Path(folder)
    bat_file = folder / "DJ.bat"

    if not folder.exists():
        print(f"Error: Folder '{folder}' does not exist.")
        return

    if bat_file.exists():
        bat_file.unlink()

    for file in sorted(folder.glob(file_pattern)):
        log_file = str(file).replace('.txt', '.err')
        name = file.stem
        full_file_name = str(file)
        file_extension = file.suffix.lower()
        sheet_name = "Statement"

        if file_extension in [".xlsx", ".xls"]:
            sql_command = f"sqlcmd.exe -S {server} -d {database} -Q \"exec create_InventoryTbl_v2 '{database}','{name}'\""
            bat_script = (
                f'DJENGINE -l "{log_file}" -sc "Database=\'{full_file_name}\' ; Table=\'{sheet_name}\'" '
                f'-tc "Server=\'{server}\';Database=\'{database}\';Table=\'dbo.{name}\'" "{map_path}"'
            )
        else:
            sql_command = f"sqlcmd.exe -S {server} -d Amazon -Q \"exec [create_InventoryTbl] '{database}','{name}'\""
            try:
                result = subprocess.run(sql_command, shell=True, capture_output=True, text=True)
            except:
                print("Error occured creating table")
            bat_script = (
                f'"C:\\Program Files\\Microsoft SQL Server\\130\\DTS\\Binn\\DTExec.exe" /ISServer "{map_path}" '
                f'/server "{ssis_deployed_server}" '
                f'/SET "\\Package.Variables[User::SrcConnectionString].Properties[Value];\'{full_file_name}\'" '
                f'/SET "\\Package.Variables[User::DstServerName].Properties[Value];\'{server}\'" '
                f'/SET "\\Package.Variables[User::DstDatabaseName].Properties[Value];\'{database}\'" '
                f'/SET "\\Package.Variables[User::DstTableName].Properties[Value];\'dbo.{name}\'"'
            )

        with open(bat_file, 'a', encoding="ascii") as bf:
            bf.write(bat_script + "\n")
        print(f"Generated batch script for {file}.")

def main(zip_folder, database, server, ssis_deployed_server, map_path, files_per_dir):
    """
    Main function to handle unzipping files, splitting them into directories, and generating batch files.
    """
    zip_folder = Path(zip_folder)
    output_base_folder = r"\\ccaintranet.com\dfs-dc-01\Split\Retail\Amazon\Inventory\Extracted"
    output_base_folder=Path(output_base_folder)
    os.makedirs(output_base_folder, exist_ok=True)

    # Step 1: Unzip all files concurrently
    extracted_folders = unzip_files_concurrently(zip_folder, output_base_folder)

    # Step 2: Split extracted files into subdirectories
    for folder in extracted_folders:
        subdirectories = split_files_into_directories(folder, files_per_dir)

        # Step 3: Generate batch files for each subdirectory
        for sub_dir in subdirectories:
            generate_batch_files(sub_dir, database, server, ssis_deployed_server, map_path)

'''
zip_folder = sys.argv[1]
database = sys.argv[2]
server = sys.argv[3]
ssis_deployed_server = sys.argv[4]
map_path = sys.argv[5]
files_per_dir = int(sys.argv[6])

main(zip_folder, database, server, ssis_deployed_server, map_path, files_per_dir)
'''

zip_folder=r"\\ccaintranet.com\dfs-dc-01\Raw\Retail\Amazon\Inventory\2025-01-09"
database="AmazonDataInventory_202306"
server="amazon.etl.sql.ccaintranet.com"
ssis_deployed_server="GAD1PRTLSSIS006"
map_path="\SSISDB\Amazon\Amazon\InventoryCostAll.dtsx"
files_per_dir=4

main(zip_folder, database, server, ssis_deployed_server, map_path, files_per_dir)

