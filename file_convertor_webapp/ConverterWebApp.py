from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import os
from fastapi import Form
from file_convertor import driver
from typing import List
from commons import uniqueid
from commons import logger, compare_hash
app = FastAPI()

# Setup logger
log = logger.setup_logger(__name__)

# Serve static files (for downloads)
app.mount("/file_convertor_webapp/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")
UPLOAD_DIR = "file_convertor_webapp/static/uploads"
CONVERTED_DIR = "file_convertor_webapp/static/converted"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(CONVERTED_DIR, exist_ok=True)

# setup database
#from .database import SqliteDatabaseConnection
#dbinstance = SqliteDatabaseConnection(reset=True)

from .mysql_database import MysqlDatabaseConnection
dbinstance = MysqlDatabaseConnection(reset=True)
dbinstance.init_db(reset=True)

from .models import ConversionRequest

# unique id 
id_generator  = uniqueid.SnowflakeIDGenerator(worker_id=1)

def genericRequestRepose(msg: str) -> HTMLResponse :
    return HTMLResponse(f"""
        <html>
            <head><title>Error</title></head>
            <body>
                <h2>{msg}</h2>
                <a href='/'>Back to Home</a>
                <br>
                <a href='/list'>All Requests</a>
            </body>
        </html>
    """)

def fileNotFoundRequestRepose(request_num: int) -> HTMLResponse:
    return genericRequestRepose(f"Can't locate Output file for request : {request_num}")

def invalidRequestRepose(request_num: int):
    return genericRequestRepose(f"Invalid Request: {request_num}")

def get_request_from_database(request_num: int)  -> tuple[HTMLResponse, dict | None] | tuple[HTMLResponse, dict] | tuple[None, dict]:
    req = dbinstance.get_record(request_num)

    if not req:
        return invalidRequestRepose(request_num), req
    print(f"status : {req['status']}")
    if req["status"] != "completed":
        # The code you provided is not valid Python code. It seems to be a combination of two
        # separate terms: "requestStatusReponse" and "
        return requestStatusReponse([req]) , req

    file_path = req['converted_path']
    if not os.path.exists(file_path):
        return  fileNotFoundRequestRepose(request_num) , req

    return None, req


# Endpoint to provide available formats
@app.get("/supported_formats")
def get_formats():
    AVAILABLE_FORMATS = driver.Creator.supported_formats()
    return JSONResponse({"formats": AVAILABLE_FORMATS})


@app.get("/", response_class=HTMLResponse)
def index():
    return """
    <html>
        <head>
            <title>File Converter</title>
            <script>
                async function loadFormats() {
                    const resp = await fetch('/supported_formats');
                    const data = await resp.json();
                    const formats = data.formats;
                    let inputSelect = document.getElementById('input_format');
                    let outputSelect = document.getElementById('output_format');
                    inputSelect.innerHTML = '';
                    outputSelect.innerHTML = '';
                    formats.forEach(fmt => {
                        let opt1 = document.createElement('option');
                        opt1.value = fmt;
                        opt1.text = fmt;
                        inputSelect.appendChild(opt1);
                        let opt2 = document.createElement('option');
                        opt2.value = fmt;
                        opt2.text = fmt;
                        outputSelect.appendChild(opt2);
                    });
                }
                window.onload = loadFormats;
            </script>
        </head>
        <body>
            <h2>Upload a file to convert</h2>
            <form id="convertForm" action="/add" enctype="multipart/form-data" method="post" onsubmit="return validateForm();">
                <label>File: <input name="file" type="file"></label><br><br>
                <label>Input Format:
                    <select name="input_format" id="input_format"></select>
                </label><br><br>
                <label>Output Format:
                    <select name="output_format" id="output_format"></select>
                </label><br><br>
                <input type="submit" value="Convert">
            </form>
            <br>
            <a href='/list'>All Requests</a>
            <script>
                function getFileExtension(filename) {
                    return filename.split('.').pop().toLowerCase();
                }

                function validateForm() {
                    const fileInput = document.querySelector('input[name="file"]');
                    const inputFormat = document.getElementById('input_format').value.toLowerCase();
                    const outputFormat = document.getElementById('output_format').value.toLowerCase();

                    if (!fileInput.files.length) {
                        alert("Please select a file.");
                        return false;
                    }

                    const fileName = fileInput.files[0].name;
                    const fileExt = getFileExtension(fileName);

                    // Validation 1: input_format must match file extension
                    if (inputFormat !== fileExt) {
                        alert(`Selected input format '${inputFormat}' does not match file extension '.${fileExt}'.`);
                        return false;
                    }

                    // Validation 2: input_format and output_format can't be same
                    if (inputFormat === outputFormat) {
                        alert("Input format and output format cannot be the same.");
                        return false;
                    }

                    return true;
                }
            </script>
        </body>
    </html>
    """

@app.post("/add")
async def add(
    file: UploadFile = File(...),
    input_format: str = Form(...),
    output_format: str = Form(...)
):
    filename = file.filename or "uploaded_file"
    file_ext = os.path.splitext(filename)[1].lstrip('.').lower()

    # Validation: input_format must match file extension
    if input_format.lower() != file_ext:
        return HTMLResponse(f"""
            <html>
                <head><title>Validation Error</title></head>
                <body>
                    <h2>Error: Selected input format '{input_format}' does not match file extension '.{file_ext}'.</h2>
                    <a href='/'>Try again</a>
                </body>
            </html>
        """)

    # Validation: input_format and output_format can't be same
    if input_format.lower() == output_format.lower():
        return HTMLResponse(f"""
            <html>
                <head><title>Validation Error</title></head>
                <body>
                    <h2>Error: Input format and output format cannot be the same ('{input_format}').</h2>
                    <a href='/'>Try again</a>
                </body>
            </html>
        """)

    # new request number
    request_num = id_generator.generate_id()
    log.info(f"adding request_num: {request_num}")

    # output paths variables
    input_directory = os.path.join(UPLOAD_DIR, str(request_num))
    upload_path = os.path.join(input_directory, filename)
    # output paths variables
    output_directory = os.path.join(CONVERTED_DIR, str(request_num))
    converted_filename = f"{os.path.splitext(filename)[0]}.{output_format.lower()}"
    converted_path = os.path.join(output_directory, converted_filename)

    # prepare directory
    os.makedirs(input_directory, exist_ok=True)
    os.makedirs(output_directory, exist_ok=True)
    try : 
        log.info(f"saving uploaded file: {upload_path}")
        with open(upload_path, "wb") as f:
            content: bytes = await file.read()
            f.write(content)

        # Save request to DB
        file_hash = compare_hash.Util.compute_sha256(upload_path)
        req = ConversionRequest(
            id=request_num,
            filename=filename,
            input_format=input_format,
            output_format=output_format,
            upload_path=upload_path,
            converted_path=converted_path,
            status="pending",
            details= "",
            file_hash = file_hash
        )
        log.info(f"saving request to DB: {request_num}")
        dbinstance.add_record(req)
        log.info(f"saved request to DB: {request_num}")

        status_url = f"/status/{req.id}"
        log.info(f"status_url: {status_url}")
        # Return HTML page with download and view link
        return HTMLResponse(f"""
            <html>
                <head><title>Download Converted File</title></head>
                <body>
                    <h2>Conversion Request Submitted, Request id : {req.id}</h2>
                    <a href='{status_url}' target='_blank'>Check Status File</a>
                    <br>
                    <br>
                    <a href='/'>Convert another file</a>
                    <br>
                    <a href='/list'>All Requests</a>
                </body>
            </html>
        """)

    except Exception as e:
        # failed to 
        return HTMLResponse(f"""
            <html>
                <head><title>Conversion Failed</title></head>
                <body>
                    <h2>Failed to process conversion request :  {e}</h2>
                    <br>
                    <a href='/'>Try again</a>
                    <br>
                    <a href='/list'>All Requests</a>
                </body>
            </html>
        """)

def requestStatusReponse(reqs: List[dict]):
    records = ""
    print(f"reqs : {reqs}")
    for req in reqs:
        view_url = f"/view/{req['id']}"
        edit_url = f"/edit/{req['id']}"
        download_url = f"/download/{req['id']}"

        if req['status'] == "completed":
            records += f"\n<tr><td>{req['id']}</td><td>{req['file_hash']}</td><td>{req['filename']}</td><td>{req['input_format']}</td><td>{req['output_format']}</td><td>{req['status']}</td><td>{req['details']}</td><td><a href='{download_url}'>Download</a></td><td><a href='{view_url}'>View</a></td><td><a href='{edit_url}'>Edit</a></td></tr>"
        else:
            records += f"\n<tr><td>{req['id']}</td><td>{req['file_hash']}</td><td>{req['filename']}</td><td>{req['input_format']}</td><td>{req['output_format']}</td><td>{req['status']}</td><td>{req['details']}</td><td></td><td></td><td></td></tr>"

    return HTMLResponse(f"""
        <html>
            <head><title>Conversion Status</title></head>
            <body>
                <h2>Conversion Requests</h2>
                <table border="1" cellpadding="5" cellspacing="0">
                    <tr><th>Id</th><th>sha256</th><th>Filename</th><th>Input Format</th><th>Output Format</th><th>Status</th><th>Details</th><th>Download</th><th>View</th><th>Edit</th></tr>
                    {records}
                </table>
                <br>
                <a href='/'>Back to Home</a>
                <br>
                <a href='/list'>All Requests</a>
            </body>
        </html>
    """)


@app.get("/list")
def list():
    reqs = dbinstance.list_records()
    return requestStatusReponse(reqs)

@app.get("/status/{request_num}")
def status(request_num: int):
    response , req = get_request_from_database(request_num)
    return response


@app.get("/download/{request_num}")
def download(request_num: int):
    response , req = get_request_from_database(request_num)
    if response != None:
        return response
    
    file_path = req['converted_path']
    converted_file_name: str = os.path.basename(file_path)
    # Serve as attachment for browser download
    return FileResponse(
        file_path,
        media_type="application/octet-stream",
        filename=converted_file_name
    )

@app.get("/view/{request_num}")
def view_file(request_num: int):
    reponse , req = get_request_from_database(request_num)
    if reponse != None:
        return reponse
    
    file_path = req['converted_path']
    filename = req['filename']

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Display as plain text in HTML
        edit_url = f"/edit/{req['id']}"
        download_url = f"/download/{req['id']}"
        return HTMLResponse(f"""
            <html>
                <head><title>View Converted File</title></head>
                <body>
                    <h2>Contents of {filename}:</h2>
                    <pre style="white-space: pre-wrap; word-break: break-all;">{content}</pre>
                    <br>
                    <a href='{edit_url}'>Edit File</a>
                    <br>
                    <a href='{download_url}'>Download File</a>
                    <br>
                    <a href='/'>Back to Home</a>
                    <br>
                    <a href='/list'>All Requests</a>
                </body>
            </html>
        """)
    except Exception as e:
        return HTMLResponse(f"""
            <html>
                <head><title>Error</title></head>
                <body>
                    <h2>Could not display file: {e}</h2>
                    <a href='/'>Back to Home</a>
                </body>
            </html>
        """)

@app.get("/edit/{request_num}", response_class=HTMLResponse)
def edit_file(request_num: int):
    reponse , req = get_request_from_database(request_num)
    if reponse != None:
        return reponse
    
    file_path = req['converted_path']
    filename = req['filename']

    try:
        log.info(f"reading file: {file_path}")
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Display content in a textarea for editing
        view_url = f"/view/{req['id']}"
        download_url = f"/download/{req['id']}"
        save_url = f"/save/{req['id']}"
        return f"""
            <html>
                <head><title>Edit Converted File</title></head>
                <body>
                    <h2>Edit contents of {filename}:</h2>
                    <form action="{save_url}" method="post">
                        <textarea name="content" rows="20" cols="80">{content}</textarea><br>
                        <input type="submit" value="Save">
                    </form>
                    <br>
                    <a href='{view_url}'>View File</a>
                    <br>
                    <a href='{download_url}'>Download File</a>
                    <br>
                    <a href='/'>Back to Home</a>
                </body>
            </html>
        """
    except Exception as e:
        return f"""
            <html>
                <head><title>Error</title></head>
                <body>
                    <h2>Could not display file for editing: {e}</h2>
                    <a href='/'>Back to Home</a>
                </body>
            </html>
        """

from fastapi import Request

@app.post("/save/{request_num}", response_class=HTMLResponse)
async def save_file(request_num: int, request: Request):
    reponse , req = get_request_from_database(request_num)
    if reponse != None:
        return reponse
    
    file_path = req['converted_path']
    filename = req['filename']

    form = await request.form()
    content = form.get("content", "")

    view_url = f"/view/{req['id']}"
    download_url = f"/download/{req['id']}"
    edit_url = f"/edit/{req['id']}"
    try:

        log.info(f"saving file: {file_path}")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(str(content))
        log.info(f"saved file: {file_path}")

        return f"""
            <html>
                <head><title>File Saved</title></head>
                <body>
                    <h2>File '{filename}' saved successfully.</h2>
                    <a href='{edit_url}'>Edit Again</a>
                    <br>
                    <a href='{view_url}'>View File</a>
                    <br>
                    <a href='{download_url}'>Download File</a>
                    <br>
                    <a href='/'>Back to Home</a>
                </body>
            </html>
        """
    except Exception as e:
        return f"""
            <html>
                <head><title>Error</title></head>
                <body>
                    <h2>Could not save file: {e}</h2>
                    <a href='{edit_url}'>Try Again</a>
                    <br>
                    <a href='/'>Back to Home</a>
                </body>
            </html>
        """
