import os
import boto3
from fastapi import Request, Response
from fastapi.responses import JSONResponse

COGNITO_USER_POOL_ID = os.getenv("COGNITO_USER_POOL_ID")
COGNITO_CLIENT_ID = os.getenv("COGNITO_CLIENT_ID")
cognito_client = boto3.client("cognito-idp")

async def login_controller(request: Request, response: Response):
    print('===')
    try:
        raw_body = await request.body()
        print('Raw body:', raw_body)
        if not raw_body:
            response.status_code = 400
            return JSONResponse({"success": False, "error": "Empty request body"})
        try:
            body = await request.json()
        except Exception as json_err:
            response.status_code = 400
            return JSONResponse({"success": False, "error": f"Invalid JSON: {json_err}"})
        print('===', body)
        username = body.get("username")
        password = body.get("password")
        print('===', username, password)
        resp = cognito_client.initiate_auth(
            ClientId=COGNITO_CLIENT_ID,
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={
                "USERNAME": username,
                "PASSWORD": password
            }
        )
        return JSONResponse({"success": True, "data": resp["AuthenticationResult"]})
    except Exception as e:
        print(e)
        response.status_code = 400
        return JSONResponse({"success": False, "error": str(e)})

async def register_controller(request: Request, response: Response):
    try:
        body = await request.json()
        username = body.get("username")
        password = body.get("password")
        email = body.get("email")
        resp = cognito_client.sign_up(
            ClientId=COGNITO_CLIENT_ID,
            Username=username,
            Password=password,
            UserAttributes=[{"Name": "email", "Value": email}]
        )
        return JSONResponse({"success": True, "data": resp})
    except Exception as e:
        response.status_code = 400
        return JSONResponse({"success": False, "error": str(e)})

async def forgot_password_controller(request: Request, response: Response):
    try:
        body = await request.json()
        username = body.get("username")
        resp = cognito_client.forgot_password(
            ClientId=COGNITO_CLIENT_ID,
            Username=username
        )
        return JSONResponse({"success": True, "data": resp})
    except Exception as e:
        response.status_code = 400
        return JSONResponse({"success": False, "error": str(e)})

async def reset_password_controller(request: Request, response: Response):
    try:
        body = await request.json()
        username = body.get("username")
        confirmation_code = body.get("confirmation_code")
        new_password = body.get("new_password")
        resp = cognito_client.confirm_forgot_password(
            ClientId=COGNITO_CLIENT_ID,
            Username=username,
            ConfirmationCode=confirmation_code,
            Password=new_password
        )
        return JSONResponse({"success": True, "data": resp})
    except Exception as e:
        response.status_code = 400
        return JSONResponse({"success": False, "error": str(e)}) 