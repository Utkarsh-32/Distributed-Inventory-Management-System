import json
import os
from pathlib import Path
from urllib.parse import quote_plus

import grpc
import uuid

from fastapi import FastAPI, Form, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

import inventory_pb2
import inventory_pb2_grpc

from leader_discovery import find_leader


# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

# Comma-separated Raft node addresses used to find the current leader.
# Docker sets this to the node containers' addresses.
RAFT_ADDRESSES = [
    address.strip()
    for address in os.getenv(
        "RAFT_ADDRESSES",
        "localhost:50061,localhost:50062,localhost:50063",
    ).split(",")
    if address.strip()
]

WEB_HOST = os.getenv("WEB_HOST", "127.0.0.1")
WEB_PORT = int(os.getenv("WEB_PORT", "8000"))


# ============================================================
# FASTAPI APPLICATION
# ============================================================

app = FastAPI(
    title="Distributed Inventory Management System",
)


app.mount(
    "/static",
    StaticFiles(directory=BASE_DIR / "static"),
    name="static",
)


templates = Jinja2Templates(
    directory=BASE_DIR / "templates"
)


# ============================================================
# gRPC CONNECTION
# ============================================================

def get_stub():
    """
    Create a gRPC client stub for the Application Server.
    """
    leader_address = find_leader(RAFT_ADDRESSES)
    if leader_address is None:
        raise HTTPException(status_code=503, detail="Leader election in progress. Please try again.")
    channel = grpc.insecure_channel(
        leader_address
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
                "warehouses": [],
                "suppliers": [],
                "restock_requests": [],
                "deliveries": [],
                "low_stock_threshold": 5,

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
    # Warehouses, suppliers, restocks and deliveries
    #
    # These sections are optional: if one of them fails to load,
    # the rest of the dashboard still renders.
    # --------------------------------------------------------

    warehouse_data = get_json_data(stub, token, "warehouse_stock") or {}
    warehouses = warehouse_data.get("warehouses", [])
    low_stock_threshold = warehouse_data.get("low_stock_threshold", 5)

    stock_by_item = {
        product["item_id"]: product.get("by_warehouse", {})
        for product in warehouse_data.get("products", [])
    }

    for item in inventory:
        item["by_warehouse"] = stock_by_item.get(item["item_id"], {})

    suppliers = get_json_data(stub, token, "suppliers") or []
    restock_requests = get_json_data(stub, token, "restock_requests") or []
    deliveries = get_json_data(stub, token, "deliveries") or []

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
            "warehouses": warehouses,
            "suppliers": suppliers,
            "restock_requests": restock_requests,
            "deliveries": deliveries,
            "low_stock_threshold": low_stock_threshold,

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
# JSON READ HELPER
# ============================================================

def get_json_data(stub, token, request_type):
    """
    Fetch one of the JSON-encoded GET operations (warehouse_stock,
    suppliers, restock_requests, deliveries).

    Returns the decoded data, or None if it could not be loaded.
    """
    try:
        response = stub.get(
            inventory_pb2.GetRequest(
                token=token,
                type=request_type,
            ),
            timeout=10,
        )

    except grpc.RpcError:
        return None

    if response.status != "SUCCESS" or not response.items:
        return None

    try:
        return json.loads(response.items[0].data)

    except json.JSONDecodeError:
        return None


# ============================================================
# WRITE HELPER
# ============================================================

def post_and_redirect(
    request: Request,
    operation: str,
    data: dict,
    idempotency_key: str,
    section: str = "",
):
    """
    Send a write operation to the leader's Application Server and
    redirect back to the dashboard with the result message.
    """
    token = get_session_token(request)

    if not token:
        return RedirectResponse(
            "/login",
            status_code=303,
        )

    idempotency_key = idempotency_key.strip()

    if not idempotency_key:
        raise HTTPException(
            status_code=400,
            detail="Idempotency key is required.",
        )

    stub = get_stub()

    try:
        response = stub.post(
            inventory_pb2.PostRequest(
                token=token,
                type=operation,
                data=json.dumps(data),
                idempotency_key=idempotency_key,
            ),
            timeout=10,
        )

    except grpc.RpcError:
        status = "ERROR"
        message_text = "Application Server is unavailable. Please try again."

    else:
        if response.status == "UNAUTHORIZED":
            return RedirectResponse(
                "/logout",
                status_code=303,
            )

        status = response.status
        message_text = response.message

    message_type = "success" if status == "SUCCESS" else "error"
    anchor = f"#{section}" if section else ""

    return RedirectResponse(
        (
            f"/dashboard?"
            f"message={quote_plus(message_text)}"
            f"&message_type={message_type}"
            f"{anchor}"
        ),
        status_code=303,
    )


# ============================================================
# PLACE ORDER
# ============================================================

@app.post("/order")
def place_order(
    request: Request,
    item_id: str = Form(...),
    quantity: int = Form(...),
    idempotency_key: str = Form(...),
    warehouse_id: str = Form(""),
):
    data = {
        "item_id": item_id,
        "quantity": quantity,
    }

    # Empty means "let the server pick the best warehouse".
    if warehouse_id.strip():
        data["warehouse_id"] = warehouse_id.strip()

    return post_and_redirect(
        request,
        "order",
        data,
        idempotency_key,
        section="inventory",
    )


# ============================================================
# SUPPLIER RESTOCK REQUESTS
# ============================================================

@app.post("/restock")
def create_restock_request(
    request: Request,
    item_id: str = Form(...),
    warehouse_id: str = Form(...),
    supplier_id: str = Form(...),
    quantity: int = Form(...),
    idempotency_key: str = Form(...),
    note: str = Form(""),
):
    return post_and_redirect(
        request,
        "restock_request",
        {
            "item_id": item_id,
            "warehouse_id": warehouse_id,
            "supplier_id": supplier_id,
            "quantity": quantity,
            "note": note,
        },
        idempotency_key,
        section="restocks",
    )


@app.post("/restock/{restock_id}/status")
def update_restock_status(
    request: Request,
    restock_id: int,
    status: str = Form(...),
    idempotency_key: str = Form(...),
    note: str = Form(""),
):
    return post_and_redirect(
        request,
        "restock_status",
        {
            "restock_id": restock_id,
            "status": status,
            "note": note,
        },
        idempotency_key,
        section="restocks",
    )


# ============================================================
# DELIVERY UPDATES
# ============================================================

@app.post("/deliveries/{order_id}/status")
def update_delivery_status(
    request: Request,
    order_id: int,
    status: str = Form(...),
    idempotency_key: str = Form(...),
    note: str = Form(""),
):
    return post_and_redirect(
        request,
        "delivery_status",
        {
            "order_id": order_id,
            "status": status,
            "note": note,
        },
        idempotency_key,
        section="deliveries",
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
        f"Open http://localhost:{WEB_PORT}"
    )

    uvicorn.run(
        app,
        host=WEB_HOST,
        port=WEB_PORT,
    )