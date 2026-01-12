import uvicorn
import multiprocessing

if __name__ == "__main__":
    workers  = ( multiprocessing.cpu_count() * 2) + 1
    uvicorn.run("main:app", host="0.0.0.0" , port= 8000 , reload=True , workers=workers)