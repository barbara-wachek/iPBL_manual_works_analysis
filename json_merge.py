import io
import json
import re
from pathlib import Path
from typing import List, Dict, Any

import gspread as gs
from google.auth.transport.requests import AuthorizedSession
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# ==========================================
# GOOGLE AUTH
# ==========================================

gc = gs.oauth()

# credentials z sesji oauth
creds = gc.http_client.auth

# klient Google Drive API
drive_service = build(
    "drive",
    "v3",
    credentials=creds
)


# ==========================================
# MAIN FUNCTION
# ==========================================

def merge_google_drive_jsons(
    folder_id: str,
    output_file: str = "data/merged_culturepl_articles.json",
    filename_pattern: str = r"^culture_pl_artykuly_.*\.json$",
) -> List[Dict[str, Any]]:

    query = f"'{folder_id}' in parents and trashed = false"

    results = (
        drive_service.files()
        .list(
            q=query,
            fields="files(id, name)",
            pageSize=1000,
        )
        .execute()
    )

    files = results.get("files", [])

    regex = re.compile(filename_pattern)

    matched_files = [
        f for f in files if regex.match(f["name"])
    ]

    print(f"Znaleziono {len(matched_files)} pasujących plików.")

    merged_data = []

    for file_meta in matched_files:

        file_id = file_meta["id"]
        file_name = file_meta["name"]

        print(f"Pobieram: {file_name}")

        request = drive_service.files().get_media(
            fileId=file_id
        )

        fh = io.BytesIO()

        downloader = MediaIoBaseDownload(
            fh,
            request
        )

        done = False

        while not done:
            _, done = downloader.next_chunk()

        fh.seek(0)

        content = fh.read().decode("utf-8")

        try:

            data = json.loads(content)

            if isinstance(data, list):
                merged_data.extend(data)

            elif isinstance(data, dict):
                merged_data.append(data)

            else:
                print(
                    f"Pomijam nieobsługiwany format: {file_name}"
                )

        except json.JSONDecodeError:
            print(
                f"Błąd parsowania JSON: {file_name}"
            )

    # ==========================================
    # SAVE
    # ==========================================

    output_path = Path(output_file)

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        output_path,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            merged_data,
            f,
            ensure_ascii=False,
            indent=2,
        )

    print("\nZapisano scalony plik:")
    print(output_path)

    print(f"\nŁączna liczba rekordów: {len(merged_data)}")

    return merged_data


# ==========================================
# RUN
# ==========================================

if __name__ == "__main__":

    merge_google_drive_jsons(
        folder_id="1M4E59ZK2BZKosDOIttgNuibPrRmjRTKy"
    )