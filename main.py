from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

# Initialize the FastAPI application
app = FastAPI(
    title="UFDR Forensic Analyzer API",
    description="API for ingesting and querying forensic extraction reports.",
    version="1.0.0"
)

@app.get("/health", tags=["System"])
async def health_check():
    """
    Basic health check endpoint to verify the API is running.
    """
    return JSONResponse(
        status_code=200,
        content={
            "status": "online",
            "service": "ufdr-api",
            "message": "FastAPI is running successfully."
        }
    )

if __name__ == "__main__":
    # This block allows you to run the file directly via `python main.py`
    uvicorn.run("main.py:app", host="0.0.0.0", port=8000, reload=True)