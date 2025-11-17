from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
from Word_dictionary_without_DB.indexed_dictionary import IndexedWordDictionaryLookup  # Use indexed for speed

app = FastAPI(title="Word Dictionary Web Service")

# Initialize the dictionary (assuming file exists; create one if needed)
def get_manifest_file():
    manifest_file = "Word_dictionary_without_DB/data/manifest.txt"
    return manifest_file

MANIFEST_FILE = get_manifest_file()
dict_lookup = IndexedWordDictionaryLookup(MANIFEST_FILE)

# Templates
templates = Jinja2Templates(directory="Word_dictionary_without_DB/templates")
os.makedirs("Word_dictionary_without_DB/templates", exist_ok=True)

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

from threading import Lock
lock = Lock()
@app.get("/refresh/dictionary")
async def post():
    try:
        with lock:
            dict_lookup.load_index()  # Reload the index from file
        return {"status": "Dictionary refreshed successfully"}  
    except RuntimeError:
        raise HTTPException(status_code=404, detail="Word not found")

@app.get("/meaning/{word}")
async def get_meaning(word: str):
    try:
        # block if loading index is in progress
        with lock:
            meaning = dict_lookup.get_meaning(word.lower())  # Assume case-insensitive
        return {"word": word, "meaning": meaning}
    except RuntimeError as e:
        print(f"Word not found: {word} Error: {e}")
        raise HTTPException(status_code=404, detail=f"{word} not found")

@app.on_event("shutdown")
def shutdown_event():
    dict_lookup.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)