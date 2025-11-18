from PyPDF2 import PdfReader
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Literal, Type, cast

TExtractorTypes = Literal["NPAE"]

# *
# *
# *
# *
# *
# *

def namePositionAndAmountExtractor(_text: str) -> List[Tuple[str, str, str]]:
    """
    Extracts Name, Position, and Amount from a text block.

    Args:
        _text (str): Raw text from PDF.

    Returns:
        List[Tuple[str, str, str]]: List of (Name, Position, Amount) tuples.
    """
    results: List[List[str]] = []

    # Split text into lines
    lines = _text.splitlines()

    # *
    # *

    for line in lines:
        line_split = line.split(" ")

        name = []
        position = []
        salary = []

        appendMode: Literal["name", "last_name", "position", "salary"] = "name"

        # *
        # *

        for i, word in enumerate(line_split):
          if appendMode == "name":
            if len(word) > 0:
              name.append(word)
            else :
              appendMode = "last_name"
              continue

          if appendMode == "last_name":
            appendMode="position"
            name.append(word)
            continue

          if i != len(line_split) - 1 :
            if appendMode == "position":
              position.append(word)
          else :
            salary.append(word)

        # *
        # *

        if len(name) > 0 and len(position) > 0 and len(salary) > 0:
          results.append([" ".join(i) for i in [name, position, salary]])

    # *
    # *

    return results


def processPage(extractedText: str, pageIndex: int, baseDir: str, extractorType: TExtractorTypes) -> str:
    """
    Saves extracted data to txt.

    Args:
        extractedText (str): Text already extracted from PDF page.
        pageIndex (int): Page number for txt naming.
        baseDir (str): Directory to save txt.

    Returns:
        str: txt file path.
    """

    # *
    # *

    if extractorType == "NPAE":
      rows = namePositionAndAmountExtractor(extractedText)

    txtPath = f"{baseDir}/{pageIndex}.txt"
    with open(txtPath, "w", newline="", encoding="utf-8") as f:
        for row in rows:
            # Write as Name||Position||Amount
            f.write(f"{row[0]}||{row[1]}||{row[2]}\n")

    # *
    # *

    print(f"[Thread] Saved → {txtPath}")
    return txtPath

# *
# *
# *
# *
# *
# *

def loopPages(pdfPath: str, startPage: int, endPage: int, max_workers: int, extractorType: TExtractorTypes):
    reader = PdfReader(pdfPath)
    totalPages = len(reader.pages)

    startPage = max(1, startPage)
    endPage = min(totalPages, endPage)

    filename = os.path.basename(pdfPath).replace(".pdf", "")
    baseDir = f"output/{filename}"
    os.makedirs(baseDir, exist_ok=True)

    # *
    # *

    txt_files = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for pageNum in range(startPage - 1, endPage):
            page = reader.pages[pageNum]
            text = page.extract_text()  # extract text here

            futures[executor.submit(processPage, text, pageNum + 1, baseDir, extractorType)] = pageNum

        for future in as_completed(futures):
            txt_files.append(future.result())

    txt_files.sort()

    # *
    # *

    # Write read.txt
    readPath = f"{baseDir}/read.txt"
    with open(readPath, "w", encoding="utf-8") as f:
        for c in sorted(txt_files):
            f.write(c + "\n")

    print(f"Created → {readPath}")


# *
# *
# *
# *
# *
# *

# Usage
# 15, 247
loopPages("./inputs/CY-2024.pdf", startPage=248, endPage=248, max_workers=10, extractorType="NPAE")
