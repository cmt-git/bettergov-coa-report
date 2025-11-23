import os
from PyPDF2 import PdfReader, PdfWriter
from datetime import datetime

# ---- CONFIG ----
PDF_FILE = "./inputs/CY-2020.pdf"   # your single PDF file
OUTPUT_ROOT = "./separator"
CHUNK_SIZE = 75

os.makedirs(OUTPUT_ROOT, exist_ok=True)

def split_pdf(input_path, start_page, end_page):
    reader = PdfReader(input_path)
    total_pages = len(reader.pages)

    # clamp within limits
    start_page = max(1, start_page)
    end_page = min(end_page, total_pages)

    filename_no_ext = os.path.splitext(os.path.basename(input_path))[0]
    file_output_folder = os.path.join(OUTPUT_ROOT, filename_no_ext)
    os.makedirs(file_output_folder, exist_ok=True)

    # convert to 0-based
    start_page -= 1

    for page in range(start_page, end_page, CHUNK_SIZE):
        writer = PdfWriter()

        chunk_start = page
        chunk_end = min(page + CHUNK_SIZE, end_page)

        # add pages
        for p in range(chunk_start, chunk_end):
            writer.add_page(reader.pages[p])

        # e.g. "15-89"
        folder_name = f"{chunk_start + 1}-{chunk_end}"
        folder_path = os.path.join(file_output_folder, folder_name)
        os.makedirs(folder_path, exist_ok=True)

        # file paths: PDF + TXT both have exact same name
        pdf_file = os.path.join(folder_path, f"{folder_name}.pdf")
        txt_file = os.path.join(folder_path, f"{folder_name}.txt")

        # create PDF
        with open(pdf_file, "wb") as f:
            writer.write(f)

        print(f"Saved PDF: {pdf_file}")

        # create TXT
        with open(txt_file, "w", encoding="utf-8") as t:
            t.write(f"Source PDF: {filename_no_ext}.pdf\n")
            t.write(f"Page Range: {folder_name}\n")
            t.write(f"Created: {datetime.now()}\n")

        print(f"Saved TXT: {txt_file}")


if __name__ == "__main__":
    start_page = int(input("Enter start page: "))
    end_page = int(input("Enter end page: "))

    print(f"\nSplitting pages {start_page} to {end_page}\n")
    split_pdf(PDF_FILE, start_page, end_page)
    print("\nDONE.")
