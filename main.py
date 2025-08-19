from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
from dotenv import load_dotenv

# Dotenv
try:
    load_dotenv('.env')
except:
    pass

# Import routers
from routers import langchain
from routers import audio
# from routers import ebooks
from routers.users import users, progress_and_rewards_routes
from routers.books import books, quiz_routes, highlights
from routers.analysis import analysis
from routers import auth
from routers import user_auth
from routers import admins_route
from routers import books_route
from routers import schools_route
from routers import users_route
from routers import analytics_route

app = FastAPI()

# TODO: add specific origins here
origins = ['*']

app.add_middleware(
  CORSMiddleware,
  allow_origins=origins,
  allow_credentials=True,
  allow_methods=['*'],
  allow_headers=['*'],
)

# Add routers to app
app.include_router(langchain.router)
app.include_router(audio.router)
# app.include_router(ebooks.router)
app.include_router(users.router)
app.include_router(highlights.router)
app.include_router(books.router)
app.include_router(quiz_routes.router)
app.include_router(progress_and_rewards_routes.router)
app.include_router(analysis.router)
app.include_router(auth.router)
app.include_router(user_auth.router)
app.include_router(admins_route.router)
app.include_router(books_route.router)
app.include_router(schools_route.router)
app.include_router(users_route.router)
app.include_router(analytics_route.router)

app.mount('/static', StaticFiles(directory='./static'), name='static')

# Root route
@app.get('/')
async def root():
    return {'message': 'AI Doge API Service'}

# Health check endpoint
@app.get('/health')
async def health_check():
    return {'status': 'healthy'}

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    exc_str = str(exc).replace('\n', ' ').replace('   ', ' ')
    print(f'{request}: {exc_str}')
    content = {'status_code': 10422, 'message': exc_str, 'data': None}
    return JSONResponse(content=content, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host='0.0.0.0',
        port=8000,
        log_level='info',
        reload=True
    )
