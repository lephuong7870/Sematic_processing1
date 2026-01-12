from fastapi import FastAPI, HTTPException, Response, Query , Body  , Path
from fastapi import FastAPI, Request
import os
from datetime import datetime
import logging
from functionReplace import replaceFunction
from functionSematic import functionMain
app = FastAPI(title="Sematic Text API")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('Create text embeding')



@app.post("/convertTexting")
async def read_json(request: Request):
    data = await request.json()  
 
    try:
        data = replaceFunction( data_sample= data)
       
        text =  functionMain( data= data) 
        logger.info("Complete ...") 
        return text
    except Exception as e:
        logger.error(f"❌ Có lỗi xảy ra: {e}", exc_info=True)
        return ""