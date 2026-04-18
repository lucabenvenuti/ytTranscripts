# YTSystem

A local system for downloading, processing, and generating transcripts, summaries, and reports from YouTube content.

---

## Project Structure

```text
C:\YTSystem\
├─ apps\
│  ├─ yt_transcript_collector\
│  └─ yt_summary_pdf_generator\
├─ config\
├─ data\
│  ├─ transcripts\
│  ├─ summaries\
│  ├─ audio_cache\
│  ├─ pdf\
│  ├─ reports\
│  ├─ plots\
│  └─ logs\
├─ temp\
├─ db\
├─ requirements.txt
├─ run_transcript_collector.bat
└─ run_summary_pdf_generator.bat
```

---

## Python Runtime

This project is expected to use:

```text
C:\Tools\python\Scripts\python.exe
```

Create the runtime and install dependencies with:

```cmd
python -m venv C:\Tools\python
C:\Tools\python\Scripts\python.exe -m pip install --upgrade pip
C:\Tools\python\Scripts\python.exe -m pip install -r C:\YTSystem\requirements.txt
```

---

## Run The Apps

Transcript collector:

```cmd
C:\YTSystem\run_transcript_collector.bat
```

Summary generator:

```cmd
C:\YTSystem\run_summary_pdf_generator.bat
```

You can also run them directly with the pinned runtime:

```cmd
C:\Tools\python\Scripts\python.exe C:\YTSystem\apps\yt_transcript_collector\main.py
C:\Tools\python\Scripts\python.exe C:\YTSystem\apps\yt_summary_pdf_generator\main.py
```

---

## Updating Other Code

When you add another app, follow the same pattern:

1. Put new Python dependencies in `C:\YTSystem\requirements.txt`.
2. Create a small `.bat` launcher that calls `C:\Tools\python\Scripts\python.exe` explicitly.
3. Point Task Scheduler to that launcher instead of plain `python`.
4. If the app loads config or channels, keep it runnable from `C:\YTSystem` and avoid hidden user-specific paths.

Avoid using plain `python` or plain `pip` in scheduled jobs, because they may resolve to a different interpreter than `C:\Tools\python`.

---

## Reset System

Full reset is handled via:

```text
C:\YTSystem\reset_all.bat
```

This removes generated data, logs, temp files, and the database. Config files are left intact.
