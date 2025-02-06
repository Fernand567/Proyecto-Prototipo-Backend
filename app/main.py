import uvicorn
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from fastapi.security import OAuth2PasswordBearer
from app.api.routes import router

app = FastAPI(default_response_class=ORJSONResponse)
app.include_router(router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
