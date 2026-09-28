from urllib.parse import quote_plus

import grpc
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

import inventory_pb2
import inventory_pb2_grpc


APP_SERVER_ADDRESS = "localhost:50052"


app = FastAPI(
    title="Distributed Inventory Management System",
)

app.mount(
    "/static",
    StaticFiles(directory="static"),
    name="static",
)


templates = Jinja2Templates(
    directory="templates"
)



# ============================================================
# gRPC CONNECTION
# ============================================================

def get_stub():
    """
    Create a gRPC client stub for the Application Server.
    """

    channel = grpc.insecure_channel(
        APP_SERVER_ADDRESS
    )

    return inventory_pb2_grpc.ClientServiceStub(
        channel
    )


# ============================================================
# AUTHENTICATION HELPER
# ============================================================

def get_session_token(request: Request):
    """
    Read the Application Server session token
    from the browser cookie.
    """

    return request.cookies.get(
        "session_token"
    )


# ============================================================
# LOGIN PAGE
# ============================================================

@app.get(
    "/login",
    response_class=HTMLResponse,
)
def login_page(
    request: Request,
):

    if get_session_token(request):

        return RedirectResponse(
            "/dashboard",
            status_code=303,
        )

    return templates.TemplateResponse(
    request=request,
    name="login.html",
    context={
        "request": request,
    },
)


# ============================================================
# LOGIN
# ============================================================

@app.post("/login")
def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):

    stub = get_stub()

    response = stub.login(
        inventory_pb2.LoginRequest(
            username=username,
            password=password,
        ),
        timeout=10,
    )

    if response.status != "SUCCESS":

        return templates.TemplateResponse(
        request=request,
        name="login.html",
        context={
            "request": request,
            "error": "Invalid username or password.",
        },
        status_code=401,
    )

    # Create browser response.
    redirect = RedirectResponse(
        "/dashboard",
        status_code=303,
    )

    # Store the server-issued token in a
    # HttpOnly cookie.
    redirect.set_cookie(
        key="session_token",
        value=response.token,
        httponly=True,
        samesite="lax",
    )

    # Username is only for display.
    redirect.set_cookie(
        key="username",
        value=username,
        httponly=False,
        samesite="lax",
    )

    return redirect


# ============================================================
# LOGOUT
# ============================================================

@app.get("/logout")
def logout(
    request: Request,
):

    token = get_session_token(request)

    if token:

        stub = get_stub()

        try:

            stub.logout(
                inventory_pb2.LogoutRequest(
                    token=token,
                ),
                timeout=10,
            )

        except grpc.RpcError:

            # Even if the Application Server is
            # temporarily unavailable, remove
            # the browser credentials.
            pass

    response = RedirectResponse(
        "/login",
        status_code=303,
    )

    response.delete_cookie(
        "session_token"
    )

    response.delete_cookie(
        "username"
    )

    return response


# ============================================================
# DASHBOARD
# ============================================================

@app.get(
    "/dashboard",
    response_class=HTMLResponse,
)
def dashboard(
    request: Request,
    message: str = "",
    message_type: str = "",
):

    token = get_session_token(request)

    if not token:

        return RedirectResponse(
            "/login",
            status_code=303,
        )

    stub = get_stub()

    # --------------------------------------------------------
    # Get inventory
    # --------------------------------------------------------

    inventory_response = stub.get(
        inventory_pb2.GetRequest(
            token=token,
            type="inventory",
        ),
        timeout=10,
    )

    # Session may have expired/been invalidated.
    if inventory_response.status == "UNAUTHORIZED":

        response = RedirectResponse(
            "/login",
            status_code=303,
        )

        response.delete_cookie(
            "session_token"
        )

        response.delete_cookie(
            "username"
        )

        return response

    inventory = []

    for item in inventory_response.items:

        # Example:
        # "Name: Laptop | Stock: 50"

        name, stock_text = item.data.split(
            " | Stock: ",
            maxsplit=1,
        )

        inventory.append(
            {
                "item_id": item.id,
                "name": name.replace(
                    "Name: ",
                    "",
                    1,
                ),
                "stock": int(stock_text),
            }
        )

    # --------------------------------------------------------
    # Get order history
    # --------------------------------------------------------

    order_response = stub.get(
        inventory_pb2.GetRequest(
            token=token,
            type="order_history",
        ),
        timeout=10,
    )

    orders = [
        item.data
        for item in order_response.items
    ]

    username = request.cookies.get(
        "username",
        "User",
    )

    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "request": request,
            "username": username,
            "inventory": inventory,
            "orders": orders,
            "message": message,
            "message_type": message_type,
        },
    )


# ============================================================
# PLACE ORDER
# ============================================================

@app.post("/order")
def place_order(
    request: Request,
    item_id: str = Form(...),
    quantity: int = Form(...),
):

    token = get_session_token(request)

    if not token:

        return RedirectResponse(
            "/login",
            status_code=303,
        )

    stub = get_stub()

    response = stub.post(
        inventory_pb2.PostRequest(
            token=token,
            type="order",
            data=(
                "{"
                f'"item_id":"{item_id}",'
                f'"quantity":{quantity}'
                "}"
            ),
        ),
        timeout=10,
    )

    if response.status == "SUCCESS":

        message_type = "success"

    else:

        message_type = "error"

    # Put the result into the redirect URL.
    message = quote_plus(
        response.message
    )

    return RedirectResponse(
        (
            f"/dashboard?"
            f"message={message}"
            f"&message_type={message_type}"
        ),
        status_code=303,
    )


# ============================================================
# ROOT
# ============================================================

@app.get("/")
def root(
    request: Request,
):

    if get_session_token(request):

        return RedirectResponse(
            "/dashboard",
            status_code=303,
        )

    return RedirectResponse(
        "/login",
        status_code=303,
    )


# ============================================================
# START SERVER
# ============================================================

if __name__ == "__main__":

    import uvicorn

    print(
        "Starting FastAPI Client Node..."
    )

    print(
        "Open http://localhost:8000"
    )

    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8000,
    )