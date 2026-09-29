import json
from urllib.parse import quote_plus

import grpc

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

import inventory_pb2
import inventory_pb2_grpc


# ============================================================
# CONFIGURATION
# ============================================================

APP_SERVER_ADDRESS = "localhost:50052"


# ============================================================
# FASTAPI APPLICATION
# ============================================================

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

    try:

        response = stub.login(
            inventory_pb2.LoginRequest(
                username=username,
                password=password,
            ),
            timeout=10,
        )

    except grpc.RpcError:

        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context={
                "request": request,
                "error": (
                    "Application Server is unavailable."
                ),
            },
            status_code=503,
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

    # Store the server-issued token in an
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
# LLM DASHBOARD HELPER
# ============================================================

def get_llm_dashboard_data(
    stub,
    token,
):
    """
    Request all LLM dashboard data in ONE gRPC call.

    The Application Server performs:
        1. Demand forecasting
        2. Reorder reasoning
        3. Inventory analytics

    The demand forecast is generated only once and reused
    by the reorder and analytics logic on the Application
    Server.

    Returns:
        (dashboard_data, error)

    Example returned data:

        {
            "forecasts": [...],
            "reorder_recommendations": [...],
            "analytics": {...}
        }
    """

    try:

        response = stub.get(
            inventory_pb2.GetRequest(
                token=token,
                type="llm_dashboard",
            ),
            timeout=900,
        )

    except grpc.RpcError as exc:

        return None, (
            f"Unable to contact Application Server: {exc}"
        )

    if response.status == "UNAUTHORIZED":

        return None, "UNAUTHORIZED"

    if response.status != "SUCCESS":

        return None, response.status

    if not response.items:

        return None, (
            "No LLM dashboard data returned."
        )

    try:

        dashboard_data = json.loads(
            response.items[0].data
        )

    except json.JSONDecodeError as exc:

        return None, (
            "Invalid JSON returned by LLM dashboard: "
            f"{exc}"
        )

    if not isinstance(
        dashboard_data,
        dict,
    ):

        return None, (
            "LLM dashboard response must be a JSON object."
        )

    return dashboard_data, None


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
    # Initialize dashboard data
    # --------------------------------------------------------

    inventory = []
    orders = []

    forecasts = []
    reorder_recommendations = []
    analytics = None

    # IMPORTANT:
    # dashboard.html expects llm_errors to be a dictionary.
    llm_errors = {
        "demand": None,
        "reorder": None,
        "analytics": None,
    }

    # --------------------------------------------------------
    # Get inventory
    # --------------------------------------------------------

    try:

        inventory_response = stub.get(
            inventory_pb2.GetRequest(
                token=token,
                type="inventory",
            ),
            timeout=10,
        )

    except grpc.RpcError:

        return templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "request": request,

                "username": request.cookies.get(
                    "username",
                    "User",
                ),

                "inventory": [],
                "orders": [],

                "forecasts": [],
                "reorder_recommendations": [],
                "analytics": None,

                "llm_errors": {
                    "demand": (
                        "Application Server is unavailable."
                    ),
                    "reorder": (
                        "Application Server is unavailable."
                    ),
                    "analytics": (
                        "Application Server is unavailable."
                    ),
                },

                "message": (
                    "Application Server is unavailable."
                ),

                "message_type": "error",
            },
            status_code=503,
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

    # --------------------------------------------------------
    # Parse inventory
    # --------------------------------------------------------

    for item in inventory_response.items:

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

    try:

        order_response = stub.get(
            inventory_pb2.GetRequest(
                token=token,
                type="order_history",
            ),
            timeout=10,
        )

    except grpc.RpcError:

        orders = []

    else:

        if order_response.status == "UNAUTHORIZED":

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

        orders = [
            item.data
            for item in order_response.items
        ]
  
    # --------------------------------------------------------
    # Username
    # --------------------------------------------------------

    username = request.cookies.get(
        "username",
        "User",
    )

    # --------------------------------------------------------
    # Render dashboard
    # --------------------------------------------------------

    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context= {
            "request": request,
            "username": username,
            "inventory": inventory,
            "orders": orders,

            "forecasts": [],
            "reorder_recommendations": [],
            "analytics": None,

            "llm_errors": {
                "demand": None,
                "reorder": None,
                "analytics": None,
            },

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

    # Build JSON safely.
    response = stub.post(
        inventory_pb2.PostRequest(
            token=token,
            type="order",
            data=json.dumps(
                {
                    "item_id": item_id,
                    "quantity": quantity,
                }
            ),
        ),
        timeout=10,
    )

    if response.status == "SUCCESS":

        message_type = "success"

    else:

        message_type = "error"

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

@app.get("/api/ai-analytics")
def ai_analytics(request: Request):

    token = get_session_token(request)

    if not token:

        return JSONResponse(
            {
                "status": "UNAUTHORIZED",
                "message": "Please log in again."
            },
            status_code=401,
        )

    stub = get_stub()

    try:

        response = stub.get(
            inventory_pb2.GetRequest(
                token=token,
                type="llm_dashboard",
            ),
            timeout=900,
        )

    except grpc.RpcError as exc:

        return JSONResponse(
            {
                "status": "ERROR",
                "message": (
                    "Unable to contact Application Server: "
                    f"{exc}"
                ),
            },
            status_code=503,
        )

    if response.status == "UNAUTHORIZED":

        return JSONResponse(
            {
                "status": "UNAUTHORIZED",
                "message": "Session expired."
            },
            status_code=401,
        )

    if response.status != "SUCCESS":

        return JSONResponse(
            {
                "status": "ERROR",
                "message": response.status,
            },
            status_code=500,
        )

    if not response.items:

        return JSONResponse(
            {
                "status": "ERROR",
                "message": "No analytics data returned."
            },
            status_code=500,
        )

    try:

        data = json.loads(
            response.items[0].data
        )

    except json.JSONDecodeError as exc:

        return JSONResponse(
            {
                "status": "ERROR",
                "message": (
                    f"Invalid analytics JSON: {exc}"
                ),
            },
            status_code=500,
        )

    return JSONResponse(
        {
            "status": "SUCCESS",
            "data": data,
        }
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