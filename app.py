from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import uvicorn

# Create FastAPI instance
app = FastAPI(
    title="My FastAPI App",
    description="A simple FastAPI application",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Root endpoint
@app.get("/")
async def read_root():
    return {"message": "Hello World", "status": "FastAPI is running!"}

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "FastAPI"}