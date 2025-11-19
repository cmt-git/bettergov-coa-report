from PyPDF2 import PdfReader
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Literal, Type, cast
from datetime import datetime
from dataclasses import dataclass

#region: detect_date_or_time
def detect_date_or_time(s: str) -> str:
    time_formats = ["%I:%M:%S%p", "%H:%M:%S", "%H:%M"]  # 12h and 24h formats
    date_formats = ["%m/%d/%Y", "%Y-%m-%d", "%d-%m-%Y"]  # common date formats

    for fmt in time_formats:
        try:
            datetime.strptime(s, fmt)
            return True
        except ValueError:
            continue

    for fmt in date_formats:
        try:
            datetime.strptime(s, fmt)
            return True
        except ValueError:
            continue

    return False

# *
# *

#region: is_number
def is_number(s: str) -> bool:
    try:
        float(s.replace(",", ""))
        return True
    except ValueError:
        return False

# *
# *
# *
# *
# *
# *

@dataclass
class ExtractorProps:
  text: str
  skipsBeforeNameExtraction: int
  skipsBeforePositionExtraction: int
  salaryOffsetIndex: int
  jrAsMiddle: bool

#region: Extractor
def Extractor(
  data: ExtractorProps
  ) -> List[Tuple[str, str, str]]:
    results: List[List[str]] = []

    # Split text into lines
    lines = data.text.splitlines()

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

        skip_threshold: int = 0
        for i, word in enumerate(line_split):

          filtered_index = i + 1

          if detect_date_or_time(word):
            continue

          if appendMode == "name" and filtered_index < skip_threshold + data.skipsBeforeNameExtraction:
            continue

          if appendMode == "name":

            word_is_not_jr = word.lower() != "jr."

            if len(word) > 0 and (word_is_not_jr or data.jrAsMiddle == False):
              word = re.sub(r"\d", "", word)

              if len(word) > 0:
                name.append(word)
              else:
                continue
            else :
              if not word_is_not_jr and data.jrAsMiddle:
                 name.append(word)

              appendMode = "last_name"
              skip_threshold = filtered_index
              continue

          if appendMode == "last_name":
            appendMode="position"
            name.append(word)
            continue

          if appendMode == "position" and filtered_index < skip_threshold + data.skipsBeforePositionExtraction:
            continue

          if len(word) > 0:
            if i != len(line_split) - (1 + data.salaryOffsetIndex) :
              if appendMode == "position":
                if is_number(word):
                  continue

                position.append(word)
            else :
              if (is_number(word)):
                salary.append(word)

        # *
        # *

        # print("\n\n\n")
        # print("="*30)
        # print(line_split)
        # print("\n")
        # print(name, position, salary)
        # print("\n")
        # print(len(name) > 0, len(position) > 0, len(salary) > 0)
        # print("="*30)

        if len(name) > 0 and len(position) > 0 and len(salary) > 0:
          results.append([" ".join(i) for i in [name, position, salary]])

    # *
    # *

    return results

# region: processPage
def processPage(
  text: str,
  pageIndex: int,
  baseDir: str,
  extractorData: ExtractorProps) -> str:
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

    extractorData.text = text
    rows = Extractor(extractorData)

    # *
    # *

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

#region: loopPages
def loopPages(
    pdfPath: str,
    startPage: int,
    endPage: int,
    max_workers: int,
    extractorData: ExtractorProps
    ):

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

            futures[executor.submit(processPage, text, pageNum + 1, baseDir, extractorData)] = pageNum

        for future in as_completed(futures):
            txt_files.append(future.result())

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

# loopPages("./inputs/CY-2024.pdf",
#           startPage=15,
#           endPage=594,
#           max_workers=10,
#           extractorData=ExtractorProps(
#             text="",
#             skipsBeforeNameExtraction=0,
#             skipsBeforePositionExtraction=0,
#             salaryOffsetIndex=0,
#             jrAsMiddle=False
#             )
#           )

# loopPages("./inputs/CY-2024.pdf",
#           startPage=596,
#           endPage=610,
#           max_workers=10,
#           extractorData=ExtractorProps(
#             text="",
#             skipsBeforeNameExtraction=2,
#             skipsBeforePositionExtraction=0,
#             salaryOffsetIndex=0,
#             jrAsMiddle=True
#             )
#           )

# loopPages("./inputs/CY-2024.pdf",
#           startPage=611,
#           endPage=633,
#           max_workers=10,
#           extractorData=ExtractorProps(
#             text="",
#             skipsBeforeNameExtraction=1,
#             skipsBeforePositionExtraction=0,
#             salaryOffsetIndex=0,
#             jrAsMiddle=True
#             )
#           )

loopPages("./inputs/CY-2024.pdf",
          startPage=635,
          endPage=635,
          max_workers=10,
          extractorData=ExtractorProps(
            text="",
            skipsBeforeNameExtraction=1,
            skipsBeforePositionExtraction=0,
            salaryOffsetIndex=2,
            jrAsMiddle=True
            )
          )

#loopPages("./inputs/CY-2024.pdf", startPage=596, endPage=596, max_workers=10, extractorType="SRO")
