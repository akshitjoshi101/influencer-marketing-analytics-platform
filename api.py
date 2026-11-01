from datetime import datetime
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from typing import Optional
import os
from urllib.parse import quote_plus

from starlette.middleware.sessions import SessionMiddleware
from jinja2 import pass_context
from kafka import KafkaProducer
import json
import uuid
from datetime import timezone

from fastapi import FastAPI, Depends, Header, HTTPException, status, Request, Form
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Text,
    Float,
    Boolean,
    ForeignKey,
    func
)
from sqlalchemy import text
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship
from werkzeug.security import generate_password_hash, check_password_hash

from recommendations import rank_influencers

from redis_cache import (
    get_cache,
    set_cache,
    delete_cache,
    clear_analytics_cache
)

from dotenv import load_dotenv
load_dotenv()

# ============================================================
# FASTAPI APPLICATION
# ============================================================

app = FastAPI(
    title="Influencer Marketing Platform API",
    description="REST API and web frontend for the Influencer Marketing Platform",
    version="2.0.0",
)

# ============================================================
# KAFKA PRODUCER
# ============================================================
producer = None

try:
    producer = KafkaProducer(
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092"
        ),
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        request_timeout_ms=3000
    )
    print("Kafka connected successfully.")

except Exception as e:
    print(f"Kafka is not running. Continuing without Kafka: {e}")
    producer = None

# ============================================================
# WEB FRONTEND SETUP
# ============================================================

app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET", "change-this-secret-key"),
)

templates = Jinja2Templates(directory="templates")

if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


@pass_context
def get_flashed_messages(context, with_categories=False):
    request = context.get("request")
    if request is None:
        return []

    messages = request.session.pop("_flashes", [])
    if with_categories:
        return messages
    return [message for _, message in messages]


def flash(request: Request, message: str, category: str = "message"):
    request.session.setdefault("_flashes", []).append((category, message))


templates.env.globals["get_flashed_messages"] = get_flashed_messages


def redirect_to(request: Request, endpoint: str, status_code: int = 303, **path_params):
    return RedirectResponse(
        url=str(request.url_for(endpoint, **path_params)),
        status_code=status_code,
    )


def frontend_user(request: Request, db: Session):
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    return db.query(User).filter(User.id == user_id).first()




# ============================================================
# DATABASE
# ============================================================

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    DB_USER = os.getenv("POSTGRES_USER", "postgres")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
    DB_PORT = os.getenv("POSTGRES_PORT", "5432")
    DB_NAME = os.getenv("POSTGRES_DB", "influencer_platform")

    if not DB_PASSWORD:
        raise RuntimeError(
            "PostgreSQL password is not configured. Set DATABASE_URL or "
            "POSTGRES_PASSWORD before starting the application."
        )

    DATABASE_URL = (
        "postgresql+psycopg2://"
        f"{quote_plus(DB_USER)}:{quote_plus(DB_PASSWORD)}"
        f"@{DB_HOST}:{DB_PORT}/{quote_plus(DB_NAME)}"
    )

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

Base = declarative_base()


def get_db():
    db = SessionLocal()

    try:
        yield db
    finally:
        db.close()


# ============================================================
# JUST A TEST ENDPOINT TO CHECK DATABASE CONNECTION
# ============================================================

@app.get("/api/raw-data-check")
def raw_data_check(db: Session = Depends(get_db)):

    # -----------------------------
    # Row counts
    # -----------------------------

    users_count = db.execute(
        text("SELECT COUNT(*) FROM raw_users")
    ).scalar()

    campaigns_count = db.execute(
        text("SELECT COUNT(*) FROM raw_campaigns")
    ).scalar()

    ad_requests_count = db.execute(
        text("SELECT COUNT(*) FROM raw_ad_requests")
    ).scalar()


    # -----------------------------
    # Users NULL checks
    # -----------------------------

    missing_usernames = db.execute(
        text("""
            SELECT COUNT(*)
            FROM raw_users
            WHERE username IS NULL
        """)
    ).scalar()

    missing_emails = db.execute(
        text("""
            SELECT COUNT(*)
            FROM raw_users
            WHERE email IS NULL
        """)
    ).scalar()


    # -----------------------------
    # Campaign NULL / invalid checks
    # -----------------------------

    missing_descriptions = db.execute(
        text("""
            SELECT COUNT(*)
            FROM raw_campaigns
            WHERE description IS NULL
        """)
    ).scalar()

    missing_budgets = db.execute(
        text("""
            SELECT COUNT(*)
            FROM raw_campaigns
            WHERE budget IS NULL
        """)
    ).scalar()

    negative_budgets = db.execute(
        text("""
            SELECT COUNT(*)
            FROM raw_campaigns
            WHERE budget < 0
        """)
    ).scalar()


    # -----------------------------
    # Ad request checks
    # -----------------------------

    missing_messages = db.execute(
        text("""
            SELECT COUNT(*)
            FROM raw_ad_requests
            WHERE messages IS NULL
        """)
    ).scalar()

    missing_payments = db.execute(
        text("""
            SELECT COUNT(*)
            FROM raw_ad_requests
            WHERE payment_amount IS NULL
        """)
    ).scalar()

    negative_payments = db.execute(
        text("""
            SELECT COUNT(*)
            FROM raw_ad_requests
            WHERE payment_amount < 0
        """)
    ).scalar()

    missing_status = db.execute(
        text("""
            SELECT COUNT(*)
            FROM raw_ad_requests
            WHERE status IS NULL
        """)
    ).scalar()


    # -----------------------------
    # Invalid foreign keys
    # -----------------------------

    invalid_influencers = db.execute(
        text("""
            SELECT COUNT(*)
            FROM raw_ad_requests
            WHERE influencer_id IN (20001, 20002, 20003)
        """)
    ).scalar()

    invalid_sponsors = db.execute(
        text("""
            SELECT COUNT(*)
            FROM raw_ad_requests
            WHERE sponsor_id IN (30001, 30002, 30003)
        """)
    ).scalar()


    # -----------------------------
    # Return results
    # -----------------------------

    return {
        "row_counts": {
            "users": users_count,
            "campaigns": campaigns_count,
            "ad_requests": ad_requests_count
        },

        "users_quality": {
            "missing_usernames": missing_usernames,
            "missing_emails": missing_emails
        },

        "campaigns_quality": {
            "missing_descriptions": missing_descriptions,
            "missing_budgets": missing_budgets,
            "negative_budgets": negative_budgets
        },

        "ad_requests_quality": {
            "missing_messages": missing_messages,
            "missing_payments": missing_payments,
            "missing_status": missing_status,
            "negative_payments": negative_payments,
            "invalid_influencer_ids": invalid_influencers,
            "invalid_sponsor_ids": invalid_sponsors
        }
    }
# ============================================================
# DATABASE MODELS
# ============================================================

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String(80), unique=True, nullable=False)
    email = Column(String(120), unique=True, nullable=False)
    password_hash = Column(String(225), nullable=False)
    role = Column(String(10), nullable=False)
    is_flagged = Column(Boolean, default=False)

    campaigns = relationship(
        "Campaign",
        back_populates="sponsor_user"
    )

    influencer_requests = relationship(
        "AdRequest",
        foreign_keys="AdRequest.influencer_id",
        back_populates="influencer"
    )

    sponsor_requests = relationship(
        "AdRequest",
        foreign_keys="AdRequest.sponsor_id",
        back_populates="sponsor"
    )


class Campaign(Base):
    __tablename__ = "campaigns"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(Text, nullable=False)
    start_date = Column(String, nullable=False)
    end_date = Column(String, nullable=False)
    budget = Column(Float, nullable=False)
    visibility = Column(String(10), nullable=False)
    is_flagged = Column(Boolean, default=False)
    sponsor_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    ad_requests = relationship(
        "AdRequest",
        back_populates="campaign"
    )

    sponsor_user = relationship(
        "User",
        back_populates="campaigns",
        primaryjoin="Campaign.sponsor_id == User.id",
        foreign_keys=[sponsor_id]
    )


class AdRequest(Base):
    __tablename__ = "ad_requests"

    id = Column(Integer, primary_key=True)
    campaign_id = Column(Integer, ForeignKey("campaigns.id"), nullable=False)
    influencer_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    sponsor_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    messages = Column(Text, nullable=True)
    requirements = Column(Text, nullable=False)
    payment_amount = Column(Float, nullable=False)
    status = Column(String(10), nullable=False)
    is_active = Column(Boolean, default=False)

    campaign = relationship(
        "Campaign",
        back_populates="ad_requests",
        primaryjoin="AdRequest.campaign_id == Campaign.id",
        foreign_keys=[campaign_id]
    )

    influencer = relationship(
        "User",
        foreign_keys=[influencer_id],
        back_populates="influencer_requests",
        primaryjoin="AdRequest.influencer_id == User.id"
    )

    sponsor = relationship(
        "User",
        foreign_keys=[sponsor_id],
        back_populates="sponsor_requests",
        primaryjoin="AdRequest.sponsor_id == User.id"
    )


# Shared workflow rules for both browser and REST routes.
ALLOWED_TRANSITIONS = {
    "new": {"accepted", "negotiate", "rejected"},
    "pending": {"accepted", "negotiate", "rejected"},
    "negotiate": {"accepted", "rejected"},
    "accepted": {"completed", "rejected"},
    "completed": {"paid"},
    "paid": set(),
    "rejected": set(),
}

def change_request_status(ad_request, target):
    current = (ad_request.status or "").lower()
    target = (target or "").lower()
    if target != current and target not in ALLOWED_TRANSITIONS.get(current, set()):
        raise HTTPException(status_code=409, detail=f"Cannot change request from {current} to {target}")
    ad_request.status = target
    if target == "rejected":
        ad_request.is_active = False

def ensure_request_editable(ad_request, requested_status, payment_amount):
    current = (ad_request.status or "").lower()
    if current in {"accepted", "completed", "paid", "rejected"}:
        raise HTTPException(status_code=409, detail="Agreed, completed, paid or rejected requests cannot be edited")
    if requested_status != current and requested_status not in {"new", "pending"}:
        raise HTTPException(status_code=409, detail="Use the negotiation action to change request status")
    if requested_status != current:
        raise HTTPException(status_code=409, detail="Editing cannot reset request status")

def ensure_campaign_deletable(db, campaign):
    if db.query(AdRequest.id).filter(AdRequest.campaign_id == campaign.id).first():
        raise HTTPException(status_code=409, detail="Cannot delete a campaign with existing ad requests")

def commit_with_events(db):
    # Snapshot changes before SQLAlchemy expires ORM objects on commit.
    from sqlalchemy import inspect
    changed = []
    for obj in list(db.new) + list(db.dirty):
        if isinstance(obj, AdRequest):
            state = inspect(obj)
            if obj in db.new or any(state.attrs[k].history.has_changes() for k in ("status", "payment_amount", "campaign_id", "influencer_id", "sponsor_id")):
                changed.append(obj)
    db.commit()
    for obj in changed:
        send_kafka_event(obj)

Base.metadata.create_all(bind=engine)

# ============================================================
# PYDANTIC SCHEMAS
# ============================================================

class UserResponse(BaseModel):
    id: int
    username: str
    email: str
    role: str
    is_flagged: bool

    model_config = ConfigDict(from_attributes=True)


class LoginRequest(BaseModel):
    username: str
    password: str


class RegisterRequest(BaseModel):
    username: str
    email: str
    password: str


class LoginResponse(BaseModel):
    message: str
    user_id: int
    username: str
    role: str
    access_token: Optional[str] = None


class CampaignResponse(BaseModel):
    id: int
    name: str
    description: str
    start_date: str
    end_date: str
    budget: float
    visibility: str
    is_flagged: bool
    sponsor_id: int

    model_config = ConfigDict(from_attributes=True)


class CampaignCreate(BaseModel):
    name: str
    description: str
    start_date: str
    end_date: str
    budget: float
    visibility: str


class CampaignUpdate(BaseModel):
    name: str
    description: str
    start_date: str
    end_date: str
    budget: float
    visibility: str


class AdRequestResponse(BaseModel):
    id: int
    campaign_id: int
    influencer_id: int
    sponsor_id: int
    messages: Optional[str]
    requirements: str
    payment_amount: float
    status: str
    is_active: bool

    model_config = ConfigDict(from_attributes=True)


class AdRequestCreate(BaseModel):
    influencer_id: int
    messages: Optional[str] = None
    requirements: str
    payment_amount: float


class AdRequestUpdate(BaseModel):
    messages: Optional[str] = None
    requirements: str
    payment_amount: float
    status: str


class BudgetRequest(BaseModel):
    new_budget: float


class InfluencerRequest(BaseModel):
    influencer_id: int
    message: Optional[str] = None


# ============================================================
# AUTHENTICATION HELPER
# Temporary replacement for Flask session
#
# Later this will become JWT authentication.
# ============================================================

token_serializer = URLSafeTimedSerializer(os.environ.get("SESSION_SECRET", "change-this-secret-key"), salt="api-auth")

def get_current_user(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    db: Session = Depends(get_db)
):
    if not authorization and not request.session.get("user_id"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Login session or Bearer token required"
        )

    user_id = request.session.get("user_id")
    if authorization:
        if not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Invalid authorization scheme")
        try:
            user_id = token_serializer.loads(authorization[7:], max_age=86400)
        except (BadSignature, SignatureExpired):
            raise HTTPException(status_code=401, detail="Invalid or expired token")
    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )

    return user


def require_role(required_role: str):

    def checker(
        current_user: User = Depends(get_current_user)
    ):
        if current_user.role != required_role:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"{required_role} access required"
            )

        return current_user

    return checker


# ============================================================
# BASIC
# ============================================================

@app.get("/", name="home_page")
def home_page(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")

# ============================================================
# AUTHENTICATION
# ============================================================

@app.post("/api/login", response_model=LoginResponse)
def api_login(
    data: LoginRequest,
    db: Session = Depends(get_db)
):

    user = (
        db.query(User)
        .filter(User.username == data.username)
        .first()
    )

    if not user:
        raise HTTPException(
            status_code=401,
            detail="Invalid username or password"
        )

    if not check_password_hash(
        user.password_hash,
        data.password
    ):
        raise HTTPException(
            status_code=401,
            detail="Invalid username or password"
        )

    return {
        "message": "Login successful",
        "access_token": token_serializer.dumps(user.id),
        "user_id": user.id,
        "username": user.username,
        "role": user.role
    }


# ============================================================
# USER REGISTRATION
# ============================================================

@app.post(
    "/api/register/{role}",
    response_model=UserResponse,
    status_code=201
)
def register_user(
    role: str,
    data: RegisterRequest,
    db: Session = Depends(get_db)
):

    if role not in {"admin", "sponsor", "influencer"}:
        raise HTTPException(
            status_code=400,
            detail="Invalid role"
        )

    existing_user = (
        db.query(User)
        .filter(
            (User.username == data.username) |
            (User.email == data.email)
        )
        .first()
    )

    if existing_user:
        raise HTTPException(
            status_code=400,
            detail="Username or email already exists"
        )

    password_hash = generate_password_hash(
        data.password
    )

    new_user = User(
        username=data.username,
        email=data.email,
        password_hash=password_hash,
        role=role
    )

    db.add(new_user)
    commit_with_events(db)
    db.refresh(new_user)

    clear_analytics_cache()

    return new_user


# ============================================================
# USERS
# ============================================================

@app.get(
    "/api/users",
    response_model=list[UserResponse]
)
def api_get_users(
    db: Session = Depends(get_db)
):

    return db.query(User).all()


@app.get(
    "/api/users/{user_id}",
    response_model=UserResponse
)
def api_get_user(
    user_id: int,
    db: Session = Depends(get_db)
):

    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(
            status_code=404,
            detail="User not found"
        )

    return user


# ============================================================
# ADMIN
# ============================================================

@app.get("/api/admin/dashboard")
def api_admin_dashboard(
    current_user: User = Depends(
        require_role("admin")
    ),
    db: Session = Depends(get_db)
):

    campaigns = db.query(Campaign).all()
    ad_requests = db.query(AdRequest).all()

    flagged_users = (
        db.query(User)
        .filter(User.is_flagged == True)
        .all()
    )

    flagged_campaigns = (
        db.query(Campaign)
        .filter(Campaign.is_flagged == True)
        .all()
    )

    return {
        "user": UserResponse.model_validate(current_user),
        "campaigns": campaigns,
        "ad_requests": ad_requests,
        "flagged_users": flagged_users,
        "flagged_campaigns": flagged_campaigns
    }


@app.get("/api/admin/info")
def api_admin_info(
    current_user: User = Depends(
        require_role("admin")
    ),
    db: Session = Depends(get_db)
):

    users = db.query(User).all()

    campaigns = db.query(Campaign).all()

    sponsors = (
        db.query(User)
        .filter(User.role == "sponsor")
        .all()
    )

    return {
        "users": users,
        "campaigns": campaigns,
        "sponsors": sponsors
    }


@app.post("/api/admin/users/{user_id}/flag")
def api_flag_user(
    user_id: int,
    current_user: User = Depends(
        require_role("admin")
    ),
    db: Session = Depends(get_db)
):

    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(
            status_code=404,
            detail="User not found"
        )

    user.is_flagged = True

    commit_with_events(db)

    return {
        "message": f"User {user.username} has been flagged."
    }


@app.post("/api/admin/users/{user_id}/unflag")
def api_unflag_user(
    user_id: int,
    current_user: User = Depends(
        require_role("admin")
    ),
    db: Session = Depends(get_db)
):

    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(
            status_code=404,
            detail="User not found"
        )

    user.is_flagged = False

    commit_with_events(db)

    return {
        "message": f"User {user.username} has been removed from flagged list."
    }


@app.post("/api/admin/campaigns/{campaign_id}/flag")
def api_flag_campaign(
    campaign_id: int,
    current_user: User = Depends(
        require_role("admin")
    ),
    db: Session = Depends(get_db)
):

    campaign = (
        db.query(Campaign)
        .filter(Campaign.id == campaign_id)
        .first()
    )

    if not campaign:
        raise HTTPException(
            status_code=404,
            detail="Campaign not found"
        )

    campaign.is_flagged = True

    commit_with_events(db)

    return {
        "message": f"Campaign {campaign.name} has been flagged."
    }


@app.post("/api/admin/campaigns/{campaign_id}/unflag")
def api_unflag_campaign(
    campaign_id: int,
    current_user: User = Depends(
        require_role("admin")
    ),
    db: Session = Depends(get_db)
):

    campaign = (
        db.query(Campaign)
        .filter(Campaign.id == campaign_id)
        .first()
    )

    if not campaign:
        raise HTTPException(
            status_code=404,
            detail="Campaign not found"
        )

    campaign.is_flagged = False

    commit_with_events(db)

    return {
        "message": f"Campaign {campaign.name} has been removed from flagged list."
    }


# ============================================================
# CAMPAIGNS
# ============================================================

def validate_campaign_dates(
    start_date: str,
    end_date: str
):
    if not start_date or not end_date:
        raise HTTPException(
            status_code=400,
            detail="Start date and end date are required."
        )

    try:
        start_dt = datetime.strptime(
            start_date,
            "%Y-%m-%d"
        )
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Start date is not a valid date (expected YYYY-MM-DD)."
        )

    try:
        end_dt = datetime.strptime(
            end_date,
            "%Y-%m-%d"
        )
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="End date is not a valid date (expected YYYY-MM-DD)."
        )

    if end_dt < start_dt:
        raise HTTPException(
            status_code=400,
            detail="End date cannot be before start date."
        )

    return start_date, end_date, None

@app.get(
    "/api/campaigns",
    response_model=list[CampaignResponse]
)
def api_get_campaigns(
    db: Session = Depends(get_db)
):

    return db.query(Campaign).all()


@app.get(
    "/api/campaigns/public",
    response_model=list[CampaignResponse]
)
def api_get_public_campaigns(
    db: Session = Depends(get_db)
):

    return (
        db.query(Campaign)
        .filter(
            Campaign.is_flagged == False,
            Campaign.visibility == "public"
        )
        .all()
    )


@app.get(
    "/api/campaigns/{campaign_id}",
    response_model=CampaignResponse
)
def api_get_campaign(
    campaign_id: int,
    db: Session = Depends(get_db)
):

    campaign = (
        db.query(Campaign)
        .filter(Campaign.id == campaign_id)
        .first()
    )

    if not campaign:
        raise HTTPException(
            status_code=404,
            detail="Campaign not found"
        )

    return campaign


@app.post(
    "/api/campaigns",
    response_model=CampaignResponse,
    status_code=201
)
def api_create_campaign(
    data: CampaignCreate,
    current_user: User = Depends(
        require_role("sponsor")
    ),
    db: Session = Depends(get_db)
):

    validate_campaign_dates(
        data.start_date,
        data.end_date
    )

    if data.budget <= 0:
        raise HTTPException(
            status_code=400,
            detail="Budget must be a positive number."
        )

    new_campaign = Campaign(
        name=data.name,
        description=data.description,
        start_date=data.start_date,
        end_date=data.end_date,
        budget=data.budget,
        visibility=data.visibility,
        sponsor_id=current_user.id
    )

    db.add(new_campaign)
    commit_with_events(db)
    db.refresh(new_campaign)

    clear_analytics_cache()

    return new_campaign


@app.put(
    "/api/campaigns/{campaign_id}",
    response_model=CampaignResponse
)
def api_update_campaign(
    campaign_id: int,
    data: CampaignUpdate,
    current_user: User = Depends(
        require_role("sponsor")
    ),
    db: Session = Depends(get_db)
):

    campaign = (
        db.query(Campaign)
        .filter(Campaign.id == campaign_id)
        .first()
    )

    if not campaign:
        raise HTTPException(
            status_code=404,
            detail="Campaign not found"
        )

    if campaign.sponsor_id != current_user.id:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to edit this campaign."
        )

    validate_campaign_dates(
        data.start_date,
        data.end_date
    )

    if data.budget <= 0:
        raise HTTPException(
            status_code=400,
            detail="Budget must be a positive number."
        )

    campaign.name = data.name
    campaign.description = data.description
    campaign.start_date = data.start_date
    campaign.end_date = data.end_date
    campaign.budget = data.budget
    campaign.visibility = data.visibility

    commit_with_events(db)
    db.refresh(campaign)

    clear_analytics_cache()

    return campaign


@app.delete("/api/campaigns/{campaign_id}")
def api_delete_campaign(
    campaign_id: int,
    current_user: User = Depends(
        require_role("sponsor")
    ),
    db: Session = Depends(get_db)
):

    campaign = (
        db.query(Campaign)
        .filter(Campaign.id == campaign_id)
        .first()
    )

    if not campaign:
        raise HTTPException(
            status_code=404,
            detail="Campaign not found"
        )

    if campaign.sponsor_id != current_user.id:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to delete this campaign."
        )

    has_requests = db.query(AdRequest.id).filter(AdRequest.campaign_id == campaign.id).first()
    if has_requests:
        campaign.visibility = "archived"
        message = "Campaign archived; associated request history preserved."
    else:
        db.delete(campaign)
        message = "Campaign deleted successfully!"
    commit_with_events(db)
    clear_analytics_cache()
    return {"message": message}


@app.get("/api/sponsors/{sponsor_id}/campaigns")
def api_get_sponsor_campaigns(
    sponsor_id: int,
    db: Session = Depends(get_db)
):

    return (
        db.query(Campaign)
        .filter(Campaign.sponsor_id == sponsor_id)
        .all()
    )


# ============================================================
# AD REQUESTS
# ============================================================

@app.get(
    "/api/ad-requests",
    response_model=list[AdRequestResponse]
)
def api_get_ad_requests(
    db: Session = Depends(get_db)
):

    return db.query(AdRequest).all()


@app.get(
    "/api/ad-requests/{ad_request_id}",
    response_model=AdRequestResponse
)
def api_get_ad_request(
    ad_request_id: int,
    db: Session = Depends(get_db)
):

    ad_request = (
        db.query(AdRequest)
        .filter(AdRequest.id == ad_request_id)
        .first()
    )

    if not ad_request:
        raise HTTPException(
            status_code=404,
            detail="Ad request not found"
        )

    return ad_request


# ============================================================
# CREATE AD REQUEST - SPONSOR
# ============================================================

@app.post(
    "/api/campaigns/{campaign_id}/ad-requests",
    response_model=AdRequestResponse,
    status_code=201
)
def api_create_ad_request(
    campaign_id: int,
    data: AdRequestCreate,
    current_user: User = Depends(
        require_role("sponsor")
    ),
    db: Session = Depends(get_db)
):

    campaign = (
        db.query(Campaign)
        .filter(Campaign.id == campaign_id)
        .first()
    )

    if not campaign:
        raise HTTPException(
            status_code=404,
            detail="Campaign not found"
        )

    if campaign.sponsor_id != current_user.id:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to create an ad request for this campaign."
        )

    influencer = (
        db.query(User)
        .filter(
            User.id == data.influencer_id,
            User.role == "influencer",
            User.is_flagged == False
        )
        .first()
    )

    if not influencer:
        raise HTTPException(
            status_code=404,
            detail="Influencer not found"
        )

    if data.payment_amount <= 0:
        raise HTTPException(
            status_code=400,
            detail="Payment amount must be a positive number."
        )

    ad_request = AdRequest(
        campaign_id=campaign_id,
        influencer_id=data.influencer_id,
        messages=data.messages,
        requirements=data.requirements,
        payment_amount=data.payment_amount,
        status="pending",
        is_active=False,
        sponsor_id=current_user.id
    )

    db.add(ad_request)
    commit_with_events(db)
    db.refresh(ad_request)

    clear_analytics_cache()

    return ad_request


# ============================================================
# SPONSOR DASHBOARD
# ============================================================

@app.get("/api/sponsor/dashboard")
def api_sponsor_dashboard(
    current_user: User = Depends(
        require_role("sponsor")
    ),
    db: Session = Depends(get_db)
):

    user_id = current_user.id

    new_requests = (
        db.query(AdRequest)
        .filter_by(
            sponsor_id=user_id,
            status="pending",
            is_active=True
        )
        .all()
    )

    new_adrequests = (
        db.query(AdRequest)
        .filter_by(
            sponsor_id=user_id,
            status="accepted",
            is_active=True
        )
        .all()
    )

    active_campaigns = (
        db.query(Campaign)
        .filter_by(
            sponsor_id=user_id,
            visibility="public"
        )
        .all()
    )

    negotiate_requests = (
        db.query(AdRequest)
        .filter_by(
            sponsor_id=user_id,
            status="negotiate",
            is_active=False
        )
        .all()
    )

    completed_requests = (
        db.query(AdRequest)
        .filter(
            AdRequest.sponsor_id == user_id,
            AdRequest.status.in_(
                ["completed", "paid"]
            )
        )
        .all()
    )

    return {
        "user": current_user,
        "new_requests": new_requests,
        "accepted_requests": new_adrequests,
        "active_campaigns": active_campaigns,
        "negotiate_requests": negotiate_requests,
        "completed_requests": completed_requests
    }


# ============================================================
# SPONSOR AD REQUEST ACTIONS
# ============================================================

@app.post("/api/ad-requests/{request_id}/accept")
def api_sponsor_accept_request(
    request_id: int,
    current_user: User = Depends(
        require_role("sponsor")
    ),
    db: Session = Depends(get_db)
):

    ad_request = (
        db.query(AdRequest)
        .filter(AdRequest.id == request_id)
        .first()
    )

    if not ad_request or ad_request.sponsor_id != current_user.id:
        raise HTTPException(
            status_code=404,
            detail="Ad request not found."
        )

    change_request_status(ad_request, "accepted")

    commit_with_events(db)

    clear_analytics_cache()

    return {
        "message": "Ad request accepted."
    }


@app.delete("/api/ad-requests/{request_id}")
def api_sponsor_reject_request(
    request_id: int,
    current_user: User = Depends(
        require_role("sponsor")
    ),
    db: Session = Depends(get_db)
):

    ad_request = (
        db.query(AdRequest)
        .filter(AdRequest.id == request_id)
        .first()
    )

    if not ad_request or ad_request.sponsor_id != current_user.id:
        raise HTTPException(
            status_code=404,
            detail="Ad request not found."
        )

    change_request_status(ad_request, "rejected")
    commit_with_events(db)

    clear_analytics_cache()

    return {
        "message": "Ad request rejected and retained in history."
    }


@app.post("/api/ad-requests/{request_id}/negotiate")
def api_sponsor_negotiate_request(
    request_id: int,
    data: BudgetRequest,
    current_user: User = Depends(
        require_role("sponsor")
    ),
    db: Session = Depends(get_db)
):

    ad_request = (
        db.query(AdRequest)
        .filter(AdRequest.id == request_id)
        .first()
    )

    if not ad_request:
        raise HTTPException(
            status_code=404,
            detail="Ad request not found."
        )

    if ad_request.sponsor_id != current_user.id:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to negotiate this ad request."
        )

    if data.new_budget <= 0:
        raise HTTPException(
            status_code=400,
            detail="Budget must be a positive number."
        )

    ad_request.payment_amount = data.new_budget
    change_request_status(ad_request, "negotiate")
    ad_request.is_active = True

    commit_with_events(db)

    clear_analytics_cache()

    return {
        "message": "Ad request status updated to negotiate."
    }


@app.post("/api/ad-requests/{request_id}/accept-negotiation")
def api_sponsor_accept_negotiation(
    request_id: int,
    current_user: User = Depends(
        require_role("sponsor")
    ),
    db: Session = Depends(get_db)
):

    ad_request = (
        db.query(AdRequest)
        .filter(AdRequest.id == request_id)
        .first()
    )

    if not ad_request or ad_request.sponsor_id != current_user.id:
        raise HTTPException(
            status_code=404,
            detail="Ad request not found."
        )

    change_request_status(ad_request, "accepted")

    commit_with_events(db)

    clear_analytics_cache()

    return {
        "message": "Negotiation accepted successfully."
    }


@app.post("/api/ad-requests/{request_id}/reject-negotiation")
def api_sponsor_reject_negotiation(
    request_id: int,
    current_user: User = Depends(
        require_role("sponsor")
    ),
    db: Session = Depends(get_db)
):

    ad_request = (
        db.query(AdRequest)
        .filter(AdRequest.id == request_id)
        .first()
    )

    if not ad_request or ad_request.sponsor_id != current_user.id:
        raise HTTPException(
            status_code=404,
            detail="Ad request not found."
        )

    change_request_status(ad_request, "rejected")
    commit_with_events(db)

    clear_analytics_cache()

    return {
        "message": "Negotiation rejected and ad request deleted."
    }


# ============================================================
# UPDATE AD REQUEST - SPONSOR
# ============================================================

@app.put(
    "/api/ad-requests/{ad_request_id}",
    response_model=AdRequestResponse
)
def api_update_ad_request(
    ad_request_id: int,
    data: AdRequestUpdate,
    current_user: User = Depends(
        require_role("sponsor")
    ),
    db: Session = Depends(get_db)
):

    ad_request = (
        db.query(AdRequest)
        .filter(AdRequest.id == ad_request_id)
        .first()
    )

    if not ad_request:
        raise HTTPException(
            status_code=404,
            detail="Ad request not found."
        )

    if ad_request.sponsor_id != current_user.id:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to update this ad request."
        )

    if data.payment_amount <= 0:
        raise HTTPException(
            status_code=400,
            detail="Payment amount must be a positive number."
        )

    sponsor_settable_statuses = {
        "pending",
        "new",
        "negotiate",
        ad_request.status
    }

    if data.status not in sponsor_settable_statuses:
        raise HTTPException(
            status_code=400,
            detail=(
                "That status can only be set by the influencer "
                "accepting or by payment confirmation."
            )
        )

    ensure_request_editable(ad_request, data.status, data.payment_amount)
    ad_request.messages = data.messages
    ad_request.requirements = data.requirements
    ad_request.payment_amount = data.payment_amount
    change_request_status(ad_request, data.status)

    commit_with_events(db)
    db.refresh(ad_request)

    clear_analytics_cache()

    return ad_request


# ============================================================
# INFLUENCER DASHBOARD
# ============================================================

@app.get("/api/influencer/dashboard")
def api_influencer_dashboard(
    current_user: User = Depends(
        require_role("influencer")
    ),
    db: Session = Depends(get_db)
):

    user_id = current_user.id

    accepted_ad_requests = (
        db.query(AdRequest)
        .filter_by(
            influencer_id=user_id,
            status="accepted"
        )
        .all()
    )

    new_requests = (
        db.query(AdRequest)
        .filter_by(
            influencer_id=user_id,
            status="new",
            is_active=True
        )
        .all()
    )

    pending_requests = (
        db.query(AdRequest)
        .filter_by(
            influencer_id=user_id,
            status="pending",
            is_active=False
        )
        .all()
    )

    ad_requests = new_requests + pending_requests

    negotiate_ad_requests = (
        db.query(AdRequest)
        .filter(
            AdRequest.influencer_id == user_id,
            AdRequest.status == "negotiate",
            AdRequest.is_active == True
        )
        .all()
    )

    completed_ad_request = (
        db.query(AdRequest)
        .filter_by(
            influencer_id=user_id,
            status="completed"
        )
        .all()
    )

    paid_ad_request = (
        db.query(AdRequest)
        .filter_by(
            influencer_id=user_id,
            status="paid"
        )
        .all()
    )

    total_money_earned = (
        db.query(
            db.func.sum(
                AdRequest.payment_amount
            )
        )
        .filter(
            AdRequest.influencer_id == user_id,
            AdRequest.status == "paid"
        )
        .scalar()
        or 0.0
    )

    return {
        "user": current_user,
        "accepted_requests": accepted_ad_requests,
        "new_requests": ad_requests,
        "negotiate_requests": negotiate_ad_requests,
        "completed_requests": completed_ad_request,
        "paid_requests": paid_ad_request,
        "total_money_earned": total_money_earned
    }


# ============================================================
# PUBLIC CAMPAIGNS FOR INFLUENCERS
# ============================================================

@app.get(
    "/api/influencer/campaigns",
    response_model=list[CampaignResponse]
)
def api_influencer_campaigns(
    db: Session = Depends(get_db)
):

    return (
        db.query(Campaign)
        .filter(
            Campaign.is_flagged == False,
            Campaign.visibility == "public"
        )
        .all()
    )


# ============================================================
# INFLUENCER SUBMITS REQUEST
# ============================================================

@app.post(
    "/api/influencer/campaigns/{campaign_id}/request",
    response_model=AdRequestResponse,
    status_code=201
)
def api_influencer_submit_request(
    campaign_id: int,
    data: AdRequestCreate,
    current_user: User = Depends(
        require_role("influencer")
    ),
    db: Session = Depends(get_db)
):

    campaign = (
        db.query(Campaign)
        .filter(Campaign.id == campaign_id)
        .first()
    )

    if not campaign:
        raise HTTPException(
            status_code=404,
            detail="Campaign not found."
        )

    if data.payment_amount <= 0:
        raise HTTPException(
            status_code=400,
            detail="Budget must be a positive number."
        )

    new_request = AdRequest(
        campaign_id=campaign_id,
        influencer_id=current_user.id,
        sponsor_id=campaign.sponsor_id,
        messages=data.messages,
        requirements="Requirements details",
        payment_amount=data.payment_amount,
        status="pending",
        is_active=True
    )

    db.add(new_request)
    commit_with_events(db)
    db.refresh(new_request)

    clear_analytics_cache()

    return new_request


# ============================================================
# INFLUENCER AD REQUEST ACTIONS
# ============================================================

@app.post("/api/influencer/ad-requests/{ad_request_id}/accept")
def api_influencer_accept_request(
    ad_request_id: int,
    current_user: User = Depends(
        require_role("influencer")
    ),
    db: Session = Depends(get_db)
):

    ad_request = (
        db.query(AdRequest)
        .filter(AdRequest.id == ad_request_id)
        .first()
    )

    if not ad_request:
        raise HTTPException(
            status_code=404,
            detail="Ad request not found."
        )

    if ad_request.influencer_id != current_user.id:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to accept this ad request."
        )

    change_request_status(ad_request, "accepted")
    ad_request.is_active = True

    commit_with_events(db)

    clear_analytics_cache()

    return {
        "message": "Ad request accepted and added to active requests."
    }


@app.delete("/api/influencer/ad-requests/{ad_request_id}")
def api_influencer_reject_request(
    ad_request_id: int,
    current_user: User = Depends(
        require_role("influencer")
    ),
    db: Session = Depends(get_db)
):

    ad_request = (
        db.query(AdRequest)
        .filter(AdRequest.id == ad_request_id)
        .first()
    )

    if not ad_request:
        raise HTTPException(
            status_code=404,
            detail="Ad request not found."
        )

    if ad_request.influencer_id != current_user.id:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to reject this ad request."
        )

    change_request_status(ad_request, "rejected")
    commit_with_events(db)

    clear_analytics_cache()

    return {
        "message": "Ad request rejected and retained in history."
    }


@app.post(
    "/api/influencer/ad-requests/{ad_request_id}/negotiate"
)
def api_influencer_negotiate_request(
    ad_request_id: int,
    data: BudgetRequest,
    current_user: User = Depends(
        require_role("influencer")
    ),
    db: Session = Depends(get_db)
):

    ad_request = (
        db.query(AdRequest)
        .filter(AdRequest.id == ad_request_id)
        .first()
    )

    if not ad_request:
        raise HTTPException(
            status_code=404,
            detail="Ad request not found."
        )

    if ad_request.influencer_id != current_user.id:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to negotiate this request."
        )

    if data.new_budget <= 0:
        raise HTTPException(
            status_code=400,
            detail="Budget must be a positive number."
        )

    ad_request.payment_amount = data.new_budget
    change_request_status(ad_request, "negotiate")
    ad_request.is_active = True

    commit_with_events(db)

    clear_analytics_cache()

    return {
        "message": "Counter-offer sent to the sponsor."
    }


@app.post(
    "/api/influencer/ad-requests/{ad_request_id}/accept-negotiation"
)
def api_influencer_accept_negotiation(
    ad_request_id: int,
    current_user: User = Depends(
        require_role("influencer")
    ),
    db: Session = Depends(get_db)
):

    ad_request = (
        db.query(AdRequest)
        .filter(AdRequest.id == ad_request_id)
        .first()
    )

    if not ad_request:
        raise HTTPException(
            status_code=404,
            detail="Ad request not found."
        )

    if ad_request.influencer_id != current_user.id:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to accept this negotiation."
        )

    change_request_status(ad_request, "accepted")

    commit_with_events(db)

    clear_analytics_cache()

    return {
        "message": "Negotiation accepted."
    }


@app.delete(
    "/api/influencer/ad-requests/{ad_request_id}/reject-negotiation"
)
def api_influencer_reject_negotiation(
    ad_request_id: int,
    current_user: User = Depends(
        require_role("influencer")
    ),
    db: Session = Depends(get_db)
):

    ad_request = (
        db.query(AdRequest)
        .filter(AdRequest.id == ad_request_id)
        .first()
    )

    if not ad_request:
        raise HTTPException(
            status_code=404,
            detail="Ad request not found."
        )

    if ad_request.influencer_id != current_user.id:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to reject this negotiation."
        )

    change_request_status(ad_request, "rejected")
    commit_with_events(db)

    clear_analytics_cache()

    return {
        "message": "Ad request rejected and retained in history."
    }


@app.post(
    "/api/influencer/ad-requests/{ad_request_id}/complete"
)
def api_influencer_complete_request(
    ad_request_id: int,
    current_user: User = Depends(
        require_role("influencer")
    ),
    db: Session = Depends(get_db)
):

    ad_request = (
        db.query(AdRequest)
        .filter(AdRequest.id == ad_request_id)
        .first()
    )

    if not ad_request:
        raise HTTPException(
            status_code=404,
            detail="Ad request not found."
        )

    if ad_request.influencer_id != current_user.id:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to update this request."
        )

    change_request_status(ad_request, "completed")

    commit_with_events(db)

    clear_analytics_cache()

    return {
        "message": "Ad request marked as completed."
    }


# ============================================================
# PAYMENT CONFIRMATION
# ============================================================

@app.post(
    "/api/ad-requests/{request_id}/payment"
)
def api_payment_confirmation(
    request_id: int,
    current_user: User = Depends(
        require_role("sponsor")
    ),
    db: Session = Depends(get_db)
):

    ad_request = (
        db.query(AdRequest)
        .filter(AdRequest.id == request_id)
        .first()
    )

    if not ad_request:
        raise HTTPException(
            status_code=404,
            detail="Ad request not found."
        )

    if ad_request.sponsor_id != current_user.id:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to confirm payment."
        )

    change_request_status(ad_request, "paid")

    commit_with_events(db)

    clear_analytics_cache()

    return {
        "message": "Simulated payment recorded (no gateway verification)."
    }


# ============================================================
# INFLUENCER SEARCH / RECOMMENDATIONS
# ============================================================

@app.get(
    "/api/sponsor/ad-requests/{ad_request_id}/find-influencers"
)
def api_find_influencers(
    ad_request_id: int,
    current_user: User = Depends(
        require_role("sponsor")
    ),
    db: Session = Depends(get_db)
):

    ad_request = (
        db.query(AdRequest)
        .filter(AdRequest.id == ad_request_id)
        .first()
    )

    if not ad_request:
        raise HTTPException(
            status_code=404,
            detail="Ad request not found."
        )

    if ad_request.sponsor_id != current_user.id:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to view this request."
        )

    influencers = (
        db.query(User)
        .filter(
            User.role == "influencer",
            User.is_flagged == False
        )
        .all()
    )

    campaign = (
        db.query(Campaign)
        .filter(Campaign.id == ad_request.campaign_id)
        .first()
    )

    ranked_influencers = rank_influencers(
        campaign,
        influencers
    )

    return {
        "ad_request_id": ad_request_id,
        "campaign_id": campaign.id,
        "ranked_influencers": ranked_influencers
    }


# ============================================================
# SPONSOR SENDS CAMPAIGN REQUEST TO INFLUENCER
# ============================================================

@app.post(
    "/api/sponsor/campaigns/{campaign_id}/ad-requests/{ad_request_id}/send/{influencer_id}",
    response_model=AdRequestResponse,
    status_code=201
)
def api_send_campaign_request(
    campaign_id: int,
    ad_request_id: int,
    influencer_id: int,
    data: InfluencerRequest,
    current_user: User = Depends(
        require_role("sponsor")
    ),
    db: Session = Depends(get_db)
):

    campaign = (
        db.query(Campaign)
        .filter(Campaign.id == campaign_id)
        .first()
    )

    influencer = (
        db.query(User)
        .filter(User.id == influencer_id)
        .first()
    )

    if not campaign or not influencer:
        raise HTTPException(
            status_code=404,
            detail="Invalid campaign or influencer."
        )

    if campaign.sponsor_id != current_user.id:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to send requests for this campaign."
        )

    if influencer.role != "influencer" or influencer.is_flagged:
        raise HTTPException(status_code=400, detail="Recipient must be an active influencer")
    if campaign.visibility == "archived":
        raise HTTPException(status_code=409, detail="Campaign is archived")
    existing = db.query(AdRequest.id).filter(
        AdRequest.campaign_id == campaign_id,
        AdRequest.influencer_id == influencer_id,
        AdRequest.sponsor_id == current_user.id
    ).first()
    if existing:
        raise HTTPException(status_code=409, detail="A request has already been sent to this influencer for this campaign")
    ad_request = AdRequest(
        campaign_id=campaign_id,
        influencer_id=influencer_id,
        sponsor_id=current_user.id,
        messages=data.message,
        requirements=campaign.description,
        payment_amount=campaign.budget,
        status="new",
        is_active=True
    )

    db.add(ad_request)
    commit_with_events(db)
    db.refresh(ad_request)

    clear_analytics_cache()

    return ad_request

# ============================================================
# WEB FRONTEND ROUTES
# ============================================================

@app.get("/login", name="login")
def frontend_login(request: Request):
    return templates.TemplateResponse(request=request, name="login.html")


@app.get("/register", name="register")
def frontend_register(request: Request):
    return templates.TemplateResponse(request=request, name="register.html")


# ============================================================
# FRONTEND AUTHENTICATION
# ============================================================

@app.api_route("/login/admin", methods=["GET", "POST"], name="login_admin")
async def frontend_login_admin(request: Request):
    db = SessionLocal()
    try:
        if request.method == "POST":
            form = await request.form()
            username = form.get("username")
            password = form.get("password")
            user = db.query(User).filter(User.username == username).first()

            if user and check_password_hash(user.password_hash, password or ""):
                if user.role == "admin":
                    request.session["user_id"] = user.id
                    request.session["role"] = "admin"
                    flash(request, "Login successful!", "success")
                    return redirect_to(request, "admin_dashboard")
                flash(request, "Unauthorized access", "danger")
            else:
                flash(request, "Invalid username or password", "danger")

        return templates.TemplateResponse(request=request, name="login_admin.html")
    finally:
        db.close()


@app.api_route("/login/sponsor", methods=["GET", "POST"], name="login_sponsor")
async def frontend_login_sponsor(request: Request):
    db = SessionLocal()
    try:
        if request.method == "POST":
            form = await request.form()
            username = form.get("username")
            password = form.get("password")
            user = db.query(User).filter(User.username == username).first()

            if user and check_password_hash(user.password_hash, password or ""):
                if user.role == "sponsor":
                    request.session["user_id"] = user.id
                    request.session["role"] = "sponsor"
                    flash(request, "Login successful!", "success")
                    return redirect_to(request, "sponsor_dashboard")
                flash(request, "Unauthorized access", "danger")
            else:
                flash(request, "Invalid username or password", "danger")

        return templates.TemplateResponse(request=request, name="login_sponsor.html")
    finally:
        db.close()


@app.api_route("/login/influencer", methods=["GET", "POST"], name="login_influencer")
async def frontend_login_influencer(request: Request):
    db = SessionLocal()
    try:
        if request.method == "POST":
            form = await request.form()
            username = form.get("username")
            password = form.get("password")
            user = db.query(User).filter(User.username == username).first()

            if user and check_password_hash(user.password_hash, password or ""):
                if user.role == "influencer":
                    request.session["user_id"] = user.id
                    request.session["role"] = "influencer"
                    flash(request, "Login successful!", "success")
                    return redirect_to(request, "influencer_dashboard")
                flash(request, "Unauthorized access", "danger")
            else:
                flash(request, "Invalid username or password", "danger")

        return templates.TemplateResponse(request=request, name="login_influencer.html")
    finally:
        db.close()


def frontend_register_form(request: Request, db: Session, role: str, template: str, login_endpoint: str, form):
    username = form.get("username")
    email = form.get("email")
    password = form.get("password")

    existing_user = db.query(User).filter(
        (User.username == username) | (User.email == email)
    ).first()

    if existing_user:
        flash(request, "Username or email already exists", "danger")
        return None

    new_user = User(
        username=username,
        email=email,
        password_hash=generate_password_hash(password or ""),
        role=role,
    )
    db.add(new_user)
    commit_with_events(db)
    clear_analytics_cache()
    flash(request, "Registration successful!", "success")
    return redirect_to(request, login_endpoint)


@app.api_route("/register/admin", methods=["GET", "POST"], name="register_admin")
async def frontend_register_admin(request: Request):
    db = SessionLocal()
    try:
        if request.method == "POST":
            form = await request.form()
            result = frontend_register_form(request, db, "admin", "register_admin.html", "login_admin", form)
            if result:
                return result
        return templates.TemplateResponse(request=request, name="register_admin.html")
    finally:
        db.close()


@app.api_route("/register/influencer", methods=["GET", "POST"], name="register_influencer")
async def frontend_register_influencer(request: Request):
    db = SessionLocal()
    try:
        if request.method == "POST":
            form = await request.form()
            result = frontend_register_form(request, db, "influencer", "register_influencer.html", "login_influencer", form)
            if result:
                return result
        return templates.TemplateResponse(request=request, name="register_influencer.html")
    finally:
        db.close()


@app.api_route("/register/sponsor", methods=["GET", "POST"], name="register_sponsor")
async def frontend_register_sponsor(request: Request):
    db = SessionLocal()
    try:
        if request.method == "POST":
            form = await request.form()
            result = frontend_register_form(request, db, "sponsor", "register_sponsor.html", "login_sponsor", form)
            if result:
                return result
        return templates.TemplateResponse(request=request, name="register_sponsor.html")
    finally:
        db.close()


# ============================================================
# FRONTEND ADMIN
# ============================================================

@app.get("/admin_dashboard", name="admin_dashboard")
def frontend_admin_dashboard(request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "admin":
            flash(request, "Please log in to access this page", "danger")
            return redirect_to(request, "login_admin")

        user = frontend_user(request, db)
        if not user:
            flash(request, "User not found", "danger")
            return redirect_to(request, "login_admin")

        campaigns = db.query(Campaign).all()
        ad_requests = db.query(AdRequest).all()
        flagged_users = db.query(User).filter(User.is_flagged == True).all()
        flagged_campaigns = db.query(Campaign).filter(Campaign.is_flagged == True).all()

        return templates.TemplateResponse(
            request=request, name="admin_dashboard.html",
            context={"user": user, "campaigns": campaigns, "ad_requests": ad_requests,
                     "flagged_users": flagged_users, "flagged_campaigns": flagged_campaigns}
        )
    finally:
        db.close()


@app.get("/admin_dashboard/info", name="admin_dashboard_info")
def frontend_admin_dashboard_info(request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "admin":
            flash(request, "Please log in as admin to access this page", "danger")
            return redirect_to(request, "login_admin")
        users = db.query(User).all()
        campaigns = db.query(Campaign).all()
        sponsors = db.query(User).filter(User.role == "sponsor").all()
        return templates.TemplateResponse(
            request=request, name="admin_dashboard_info.html",
            context={"users": users, "campaigns": campaigns, "sponsors": sponsors}
        )
    finally:
        db.close()


@app.post("/remove_flagged_user/{user_id}", name="remove_flagged_user")
def frontend_remove_flagged_user(user_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "admin":
            flash(request, "Please log in as admin to access this page", "danger")
            return redirect_to(request, "login_admin")
        user = db.query(User).filter(User.id == user_id).first()
        if user:
            user.is_flagged = False
            commit_with_events(db)
            flash(request, f"User {user.username} has been removed from flagged list.", "success")
        return redirect_to(request, "admin_dashboard")
    finally:
        db.close()


@app.post("/remove_flagged_campaign/{campaign_id}", name="remove_flagged_campaign")
def frontend_remove_flagged_campaign(campaign_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "admin":
            flash(request, "Please log in as admin to access this page", "danger")
            return redirect_to(request, "login_admin")
        campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
        if campaign:
            campaign.is_flagged = False
            commit_with_events(db)
            flash(request, f"Campaign {campaign.name} has been removed from flagged list.", "success")
        return redirect_to(request, "admin_dashboard")
    finally:
        db.close()


@app.post("/flag_campaign/{campaign_id}", name="flag_campaign")
def frontend_flag_campaign(campaign_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "admin":
            flash(request, "Please log in as admin to access this page", "danger")
            return redirect_to(request, "login_admin")
        campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
        if campaign:
            campaign.is_flagged = True
            commit_with_events(db)
            flash(request, f"Campaign {campaign.name} has been flagged.", "success")
        return redirect_to(request, "admin_dashboard_info")
    finally:
        db.close()


@app.post("/flag_user/{user_id}", name="flag_user")
def frontend_flag_user(user_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "admin":
            flash(request, "Please log in as admin to access this page", "danger")
            return redirect_to(request, "login_admin")
        user = db.query(User).filter(User.id == user_id).first()
        if user:
            user.is_flagged = True
            commit_with_events(db)
            flash(request, f"User {user.username} has been flagged.", "success")
        return redirect_to(request, "admin_dashboard_info")
    finally:
        db.close()


# ============================================================
# FRONTEND INFLUENCER
# ============================================================

@app.get("/influencer_dashboard", name="influencer_dashboard")
def frontend_influencer_dashboard(request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "influencer":
            flash(request, "Please log in to access this page", "danger")
            return redirect_to(request, "login_influencer")
        user_id = request.session.get("user_id")
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            flash(request, "User not found", "danger")
            return redirect_to(request, "login_influencer")
        accepted_ad_requests = db.query(AdRequest).filter_by(influencer_id=user_id, status="accepted").all()
        ad_requests = (
            db.query(AdRequest).filter_by(influencer_id=user_id, status="new", is_active=True).all()
            + db.query(AdRequest).filter_by(influencer_id=user_id, status="pending", is_active=False).all()
        )
        negotiate_ad_requests = db.query(AdRequest).filter(
            AdRequest.influencer_id == user_id, AdRequest.status == "negotiate", AdRequest.is_active == True
        ).all()
        completed_ad_request = db.query(AdRequest).filter_by(influencer_id=user_id, status="completed").all()
        paid_ad_request = db.query(AdRequest).filter_by(influencer_id=user_id, status="paid").all()
        total_money_earned = db.query(func.sum(AdRequest.payment_amount)).filter(
            AdRequest.influencer_id == user_id, AdRequest.status == "paid"
        ).scalar() or 0.0
        return templates.TemplateResponse(
            request=request, name="influencer_dashboard.html",
            context={"negotiate_ad_requests": negotiate_ad_requests, "user": user,
                     "accepted_ad_requests": accepted_ad_requests, "ad_requests": ad_requests,
                     "total_money_earned": total_money_earned, "completed_ad_request": completed_ad_request,
                     "paid_ad_request": paid_ad_request}
        )
    finally:
        db.close()


@app.get("/influencer_dashboard/find", name="influencer_dashboard_info")
def frontend_influencer_dashboard_info(request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "influencer":
            flash(request, "Please log in to access this page", "danger")
            return redirect_to(request, "login_influencer")
        campaigns = db.query(Campaign).filter(Campaign.is_flagged == False, Campaign.visibility == "public").all()
        return templates.TemplateResponse(request=request, name="influencer_dashboard_find.html", context={"campaigns": campaigns})
    finally:
        db.close()


@app.post("/submitt_requestt/{campaign_id}", name="submitt_requestt")
async def frontend_submitt_requestt(campaign_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "influencer":
            flash(request, "Please log in to access this page", "danger")
            return redirect_to(request, "login_influencer")
        form = await request.form()
        influencer_id = request.session.get("user_id")
        campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
        if not campaign:
            flash(request, "Campaign not found.", "danger")
            return redirect_to(request, "influencer_dashboard_info")
        try:
            parsed_budget = float(form.get("influencer_budget"))
            if parsed_budget <= 0:
                raise ValueError
        except (ValueError, TypeError):
            flash(request, "Budget must be a valid positive number.", "danger")
            return redirect_to(request, "influencer_dashboard_info")
        new_request = AdRequest(
            campaign_id=campaign_id, influencer_id=influencer_id, sponsor_id=campaign.sponsor_id,
            messages=form.get("message"), requirements="Requirements details",
            payment_amount=parsed_budget, status="pending", is_active=True
        )
        db.add(new_request)
        commit_with_events(db)
        db.refresh(new_request)
        clear_analytics_cache()

        return redirect_to(request, "influencer_dashboard_info")
    finally:
        db.close()


def _influencer_request(request: Request, db: Session, ad_request_id: int, action: str):
    if request.session.get("role") != "influencer":
        return redirect_to(request, "login_influencer")
    ad_request = db.query(AdRequest).filter(AdRequest.id == ad_request_id).first()
    if not ad_request:
        flash(request, "Ad request not found.", "danger")
    elif ad_request.influencer_id != request.session.get("user_id"):
        flash(request, f"You do not have permission to {action} this ad request.", "danger")
    elif action == "accept":
        change_request_status(ad_request, "accepted")
        ad_request.is_active = True
        commit_with_events(db)
        clear_analytics_cache()
        flash(request, "Ad request accepted and added to active requests.", "success")
    elif action == "reject":
        change_request_status(ad_request, "rejected")
        commit_with_events(db)
        clear_analytics_cache()
        flash(request, "Ad request rejected and retained in history.", "success")
    return redirect_to(request, "influencer_dashboard")


@app.post("/ad_requests/{ad_request_id}/accept", name="accept_ad_request")
def frontend_accept_ad_request(ad_request_id: int, request: Request):
    db = SessionLocal()
    try: return _influencer_request(request, db, ad_request_id, "accept")
    finally: db.close()


@app.post("/ad_requests/{ad_request_id}/reject", name="reject_ad_request")
def frontend_reject_ad_request(ad_request_id: int, request: Request):
    db = SessionLocal()
    try: return _influencer_request(request, db, ad_request_id, "reject")
    finally: db.close()


@app.post("/negotiate_ad_requestt/{ad_request_id}", name="negotiate_ad_requestt")
async def frontend_negotiate_ad_requestt(ad_request_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "influencer":
            return redirect_to(request, "login_influencer")
        form = await request.form()
        ad_request = db.query(AdRequest).filter(AdRequest.id == ad_request_id).first()
        if not ad_request or ad_request.influencer_id != request.session.get("user_id"):
            flash(request, "Ad request not found or you do not have permission to negotiate it.", "danger")
            return redirect_to(request, "influencer_dashboard")
        try:
            parsed_budget = float(form.get("new_budget"))
            if parsed_budget <= 0: raise ValueError
        except (ValueError, TypeError):
            flash(request, "Budget must be a valid positive number.", "danger")
            return redirect_to(request, "influencer_dashboard")
        ad_request.payment_amount = parsed_budget
        change_request_status(ad_request, "negotiate")
        ad_request.is_active = False
        commit_with_events(db)
        clear_analytics_cache()
        flash(request, "Counter-offer sent to the sponsor.", "success")
        return redirect_to(request, "influencer_dashboard")
    finally:
        db.close()


@app.post("/accept_negotiation/{ad_request_id}", name="accept_negotiation")
def frontend_accept_negotiation(ad_request_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "influencer": return redirect_to(request, "login_influencer")
        ad_request = db.query(AdRequest).filter(AdRequest.id == ad_request_id).first()
        if ad_request and ad_request.influencer_id == request.session.get("user_id"):
            change_request_status(ad_request, "accepted"); commit_with_events(db); clear_analytics_cache(); flash(request, "Negotiation accepted.", "success")
        else: flash(request, "Ad request not found.", "danger")
        return redirect_to(request, "influencer_dashboard")
    finally: db.close()


@app.post("/reject_negotiation/{ad_request_id}", name="reject_negotiation")
def frontend_reject_negotiation(ad_request_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "influencer": return redirect_to(request, "login_influencer")
        ad_request = db.query(AdRequest).filter(AdRequest.id == ad_request_id).first()
        if not ad_request:
            flash(request, "Ad request not found.", "danger")
        elif ad_request.influencer_id != request.session.get("user_id"):
            flash(request, "You do not have permission to reject this negotiation.", "danger")
        else:
            change_request_status(ad_request, "rejected"); commit_with_events(db); clear_analytics_cache(); flash(request, "Ad request rejected and retained in history.", "success")
        return redirect_to(request, "influencer_dashboard")
    finally: db.close()


@app.post("/completed_ad_request/{ad_request_id}", name="completed_ad_request")
def frontend_completed_ad_request(ad_request_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "influencer": return redirect_to(request, "login_influencer")
        ad_request = db.query(AdRequest).filter(AdRequest.id == ad_request_id).first()
        if not ad_request:
            flash(request, "Ad request not found.", "danger")
        elif ad_request.influencer_id != request.session.get("user_id"):
            flash(request, "You do not have permission to update this ad request.", "danger")
        else:
            change_request_status(ad_request, "completed"); commit_with_events(db); clear_analytics_cache(); flash(request, "Ad request marked as completed.", "success")
        return redirect_to(request, "influencer_dashboard")
    finally: db.close()


# ============================================================
# FRONTEND SPONSOR
# ============================================================

@app.get("/sponsor_dashboard", name="sponsor_dashboard")
def frontend_sponsor_dashboard(request: Request):
    db = SessionLocal()
    try:
        if not request.session.get("user_id"):
            flash(request, "You must be logged in to access this page.", "error")
            return redirect_to(request, "login_sponsor")
        user_id = request.session["user_id"]
        user = db.query(User).filter(User.id == user_id).first()
        new_requests = db.query(AdRequest).filter_by(sponsor_id=user_id, status="pending", is_active=True).all()
        new_adrequests = db.query(AdRequest).filter_by(sponsor_id=user_id, status="accepted", is_active=True).all()
        active_campaigns = db.query(Campaign).filter_by(sponsor_id=user_id, visibility="public").all()
        ad_requests = db.query(AdRequest).filter_by(sponsor_id=user_id, status="negotiate", is_active=False).all()
        completed_ad_request = db.query(AdRequest).filter(AdRequest.sponsor_id == user_id, AdRequest.status.in_(["completed", "paid"])).all()
        return templates.TemplateResponse(
            request=request, name="sponsor_dashboard.html",
            context={"user": user, "new_requests": new_requests, "active_campaigns": active_campaigns,
                     "ad_requests": ad_requests, "new_adrequests": new_adrequests, "completed_ad_request": completed_ad_request}
        )
    finally: db.close()


@app.post("/accept_request/{request_id}", name="accept_request")
def frontend_accept_request(request_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor": return redirect_to(request, "login_sponsor")
        ad_request = db.query(AdRequest).filter(AdRequest.id == request_id).first()
        if not ad_request or ad_request.sponsor_id != request.session.get("user_id"):
            flash(request, "Ad request not found.", "error")
        else:
            change_request_status(ad_request, "accepted"); commit_with_events(db)
        return redirect_to(request, "sponsor_dashboard")
    finally: db.close()


@app.post("/reject_request/{request_id}", name="reject_request")
def frontend_reject_request(request_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor": return redirect_to(request, "login_sponsor")
        ad_request = db.query(AdRequest).filter(AdRequest.id == request_id).first()
        if not ad_request or ad_request.sponsor_id != request.session.get("user_id"):
            flash(request, "Ad request not found.", "error")
        else:
            change_request_status(ad_request, "rejected"); commit_with_events(db)
        return redirect_to(request, "sponsor_dashboard")
    finally: db.close()


@app.post("/negotiate_ad_request/{request_id}", name="negotiate_ad_request")
async def frontend_negotiate_ad_request(request_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor": return redirect_to(request, "login_sponsor")
        form = await request.form()
        ad_request = db.query(AdRequest).filter(AdRequest.id == request_id).first()
        if not ad_request or ad_request.sponsor_id != request.session.get("user_id"):
            flash(request, "Ad request not found or you do not have permission.", "danger")
            return redirect_to(request, "sponsor_dashboard")
        try:
            parsed_budget = float(form.get("new_budget"))
            if parsed_budget <= 0: raise ValueError
        except (ValueError, TypeError):
            flash(request, "Invalid budget amount.", "danger")
            return redirect_to(request, "sponsor_dashboard")
        ad_request.payment_amount = parsed_budget
        change_request_status(ad_request, "negotiate")
        ad_request.is_active = True
        commit_with_events(db)
        clear_analytics_cache()
        flash(request, "Ad request status updated to negotiate.", "success")
        return redirect_to(request, "sponsor_dashboard")
    finally: db.close()


@app.post("/accept_negotiationn/{ad_request_id}", name="accept_negotiationn")
def frontend_accept_negotiationn(ad_request_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor": return redirect_to(request, "login_sponsor")
        ad_request = db.query(AdRequest).filter(AdRequest.id == ad_request_id).first()
        if ad_request and ad_request.sponsor_id == request.session.get("user_id"):
            change_request_status(ad_request, "accepted"); commit_with_events(db); clear_analytics_cache(); flash(request, "Negotiation accepted successfully.", "success")
        else: flash(request, "Ad request not found.", "danger")
        return redirect_to(request, "sponsor_dashboard")
    finally: db.close()


@app.post("/reject_negotiationn/{ad_request_id}", name="reject_negotiationn")
def frontend_reject_negotiationn(ad_request_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor": return redirect_to(request, "login_sponsor")
        ad_request = db.query(AdRequest).filter(AdRequest.id == ad_request_id).first()
        if ad_request and ad_request.sponsor_id == request.session.get("user_id"):
            change_request_status(ad_request, "rejected"); commit_with_events(db); clear_analytics_cache(); flash(request, "Negotiation rejected and ad request deleted.", "success")
        else: flash(request, "Ad request not found.", "danger")
        return redirect_to(request, "sponsor_dashboard")
    finally: db.close()


@app.get("/sponsor_dashboard/find", name="sponsor_dashboard_find")
def frontend_sponsor_dashboard_find(request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor":
            flash(request, "Please log in to access this page", "danger")
            return redirect_to(request, "login_sponsor")
        sponsor_id = request.session["user_id"]
        campaigns = db.query(Campaign).filter(Campaign.is_flagged == False, Campaign.visibility == "public").all()
        my_campaigns = db.query(Campaign).filter_by(sponsor_id=sponsor_id, is_flagged=False).all()
        influencers = db.query(User).filter(User.role == "influencer", User.is_flagged == False).all()
        return templates.TemplateResponse(request=request, name="sponsor_dashboard_find.html", context={"campaigns": campaigns, "my_campaigns": my_campaigns, "influencers": influencers})
    finally: db.close()


@app.get("/sponsor_dashboard/campaign", name="sponsor_campaign")
def frontend_sponsor_campaign(request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor":
            flash(request, "Please log in to access this page", "danger")
            return redirect_to(request, "login_sponsor")
        sponsor_id = request.session["user_id"]
        campaigns = db.query(Campaign).filter(Campaign.sponsor_id == sponsor_id, Campaign.visibility != "archived").all()
        ad_requests = db.query(AdRequest).filter_by(sponsor_id=sponsor_id, status="pending", is_active=False).all()
        return templates.TemplateResponse(request=request, name="sponsor_dashboard_campaign.html", context={"campaigns": campaigns, "ad_requests": ad_requests})
    finally: db.close()


@app.post("/create_campaign", name="create_campaign")
async def frontend_create_campaign(request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor":
            flash(request, "Please log in to access this page", "danger")
            return redirect_to(request, "login_sponsor")
        form = await request.form()
        start_date, end_date, date_error = validate_campaign_dates(form.get("start_date"), form.get("end_date"))
        if date_error:
            flash(request, date_error, "danger"); return redirect_to(request, "sponsor_campaign")
        try:
            budget = float(form.get("budget"))
            if budget <= 0: raise ValueError
        except (ValueError, TypeError):
            flash(request, "Budget must be a valid positive number.", "danger"); return redirect_to(request, "sponsor_campaign")
        campaign = Campaign(name=form.get("name"), description=form.get("description"), start_date=start_date, end_date=end_date, budget=budget, visibility=form.get("visibility"), sponsor_id=request.session["user_id"])
        db.add(campaign); commit_with_events(db); clear_analytics_cache()
        flash(request, "Campaign created successfully!", "success")
        return redirect_to(request, "sponsor_campaign")
    finally: db.close()


@app.post("/delete_campaign/{campaign_id}", name="delete_campaign")
def frontend_delete_campaign(campaign_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor": return redirect_to(request, "login_sponsor")
        campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
        if not campaign: flash(request, "Campaign not found!", "danger")
        elif campaign.sponsor_id != request.session.get("user_id"): flash(request, "You do not have permission to delete this campaign.", "danger")
        else:
            has_requests = db.query(AdRequest.id).filter(AdRequest.campaign_id == campaign.id).first()
            if has_requests:
                campaign.visibility = "archived"
                flash(request, "Campaign archived; request history was preserved.", "success")
            else:
                db.delete(campaign)
                flash(request, "Campaign deleted successfully!", "success")
            commit_with_events(db)
            clear_analytics_cache()
        return redirect_to(request, "sponsor_campaign")
    finally: db.close()


@app.get("/edit_campaign/{campaign_id}", name="edit_campaign")
def frontend_edit_campaign(campaign_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor": return redirect_to(request, "login_sponsor")
        campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
        if not campaign:
            flash(request, "Campaign not found", "danger"); return redirect_to(request, "sponsor_campaign")
        if campaign.sponsor_id != request.session.get("user_id"):
            flash(request, "You do not have permission to edit this campaign.", "danger"); return redirect_to(request, "sponsor_campaign")
        return templates.TemplateResponse(request=request, name="edit_campaign.html", context={"campaign": campaign})
    finally: db.close()


@app.post("/update_campaign/{campaign_id}", name="update_campaign")
async def frontend_update_campaign(campaign_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor": return redirect_to(request, "login_sponsor")
        form = await request.form()
        campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
        if not campaign:
            flash(request, "Campaign not found", "danger"); return redirect_to(request, "sponsor_campaign")
        if campaign.sponsor_id != request.session.get("user_id"):
            flash(request, "You do not have permission to edit this campaign.", "danger"); return redirect_to(request, "sponsor_campaign")
        try:
            budget = float(form.get("budget"))
            if budget <= 0: raise ValueError
        except (ValueError, TypeError):
            flash(request, "Budget must be a valid positive number.", "danger"); return redirect_to(request, "sponsor_campaign")
        start_date, end_date, date_error = validate_campaign_dates(form.get("start_date"), form.get("end_date"))
        if date_error:
            flash(request, date_error, "danger"); return redirect_to(request, "sponsor_campaign")
        campaign.name = form.get("name"); campaign.description = form.get("description"); campaign.start_date = start_date; campaign.end_date = end_date; campaign.budget = budget; campaign.visibility = form.get("visibility")
        commit_with_events(db); clear_analytics_cache(); flash(request, "Campaign updated successfully!", "success")
        return redirect_to(request, "sponsor_campaign")
    finally: db.close()


@app.api_route("/create_ad_request/{campaign_id}", methods=["GET", "POST"], name="create_ad_request")
async def frontend_create_ad_request(campaign_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor": return redirect_to(request, "login_sponsor")
        campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
        if not campaign or campaign.sponsor_id != request.session.get("user_id"):
            flash(request, "You do not have permission to create an ad request for this campaign.", "danger"); return redirect_to(request, "sponsor_campaign")
        if request.method == "POST":
            form = await request.form()
            try:
                payment_amount = float(form.get("payment_amount"))
                if payment_amount <= 0: raise ValueError
            except (ValueError, TypeError):
                flash(request, "Payment amount must be a valid positive number.", "danger"); return redirect_to(request, "create_ad_request", campaign_id=campaign_id)
            ad_request = AdRequest(campaign_id=campaign_id, influencer_id=int(form.get("influencer_id")), messages=form.get("messages"), requirements=form.get("requirements"), payment_amount=payment_amount, status="pending", is_active=False, sponsor_id=request.session["user_id"])
            db.add(ad_request); commit_with_events(db); db.refresh(ad_request); clear_analytics_cache()
            flash(request, "Ad request created successfully!", "success")
            return redirect_to(request, "sponsor_campaign")
        influencers = db.query(User).filter(User.role == "influencer", User.is_flagged == False).all()
        return templates.TemplateResponse(request=request, name="create_ad_request.html", context={"campaign_id": campaign_id, "influencers": influencers})
    finally: db.close()


@app.post("/delete_ad_request/{ad_request_id}", name="delete_ad_request")
def frontend_delete_ad_request(ad_request_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor": return redirect_to(request, "login_sponsor")
        ad_request = db.query(AdRequest).filter(AdRequest.id == ad_request_id).first()
        if not ad_request: flash(request, "Ad request not found!", "danger")
        elif ad_request.sponsor_id != request.session.get("user_id"): flash(request, "You do not have permission to delete this ad request.", "danger")
        else: change_request_status(ad_request, "rejected"); commit_with_events(db); clear_analytics_cache(); flash(request, "Ad request deleted successfully!", "success")
        return redirect_to(request, "sponsor_dashboard")
    finally: db.close()


@app.get("/edit_ad_request/{ad_request_id}", name="edit_ad_request")
def frontend_edit_ad_request(ad_request_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor": return redirect_to(request, "login_sponsor")
        ad_request = db.query(AdRequest).filter(AdRequest.id == ad_request_id).first()
        if not ad_request:
            flash(request, "Ad request not found", "danger"); return redirect_to(request, "sponsor_campaign")
        if ad_request.sponsor_id != request.session.get("user_id"):
            flash(request, "You do not have permission to edit this ad request.", "danger"); return redirect_to(request, "sponsor_campaign")
        return templates.TemplateResponse(request=request, name="edit_ad_request.html", context={"ad_request": ad_request})
    finally: db.close()


@app.post("/update_ad_request/{ad_request_id}", name="update_ad_request")
async def frontend_update_ad_request(ad_request_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor": return redirect_to(request, "login_sponsor")
        form = await request.form()
        ad_request = db.query(AdRequest).filter(AdRequest.id == ad_request_id).first()
        if not ad_request:
            flash(request, "Ad request not found", "danger"); return redirect_to(request, "sponsor_campaign")
        if ad_request.sponsor_id != request.session.get("user_id"):
            flash(request, "You do not have permission to update this ad request.", "danger"); return redirect_to(request, "sponsor_campaign")
        try:
            payment = float(form.get("payment_amount"))
            if payment <= 0: raise ValueError
        except (ValueError, TypeError):
            flash(request, "Payment amount must be a valid positive number.", "danger"); return redirect_to(request, "edit_ad_request", ad_request_id=ad_request_id)
        requested_status = form.get("status")
        if requested_status not in {"pending", "new", "negotiate", ad_request.status}:
            flash(request, "That status can only be set by the influencer accepting or by payment confirmation.", "danger"); return redirect_to(request, "edit_ad_request", ad_request_id=ad_request_id)
        ensure_request_editable(ad_request, requested_status, payment)
        ad_request.messages = form.get("messages"); ad_request.requirements = form.get("requirements"); ad_request.payment_amount = payment; change_request_status(ad_request, requested_status)
        commit_with_events(db); clear_analytics_cache(); flash(request, "Ad request updated successfully!", "success")
        return redirect_to(request, "sponsor_campaign")
    finally: db.close()


@app.get("/find_influencers/{ad_request_id}", name="find_influencer")
def frontend_find_influencer(ad_request_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor": return redirect_to(request, "login_sponsor")
        ad_request = db.query(AdRequest).filter(AdRequest.id == ad_request_id).first()
        if not ad_request:
            flash(request, "Ad request not found.", "danger"); return redirect_to(request, "sponsor_campaign")
        if ad_request.sponsor_id != request.session.get("user_id"):
            flash(request, "You do not have permission to view this ad request.", "danger"); return redirect_to(request, "sponsor_campaign")
        influencers = db.query(User).filter(User.role == "influencer", User.is_flagged == False).all()
        ranked_influencers = rank_influencers(ad_request.campaign, influencers)
        sent_influencer_ids = {row[0] for row in db.query(AdRequest.influencer_id).filter(
            AdRequest.campaign_id == ad_request.campaign_id,
            AdRequest.sponsor_id == request.session["user_id"]
        ).all()}
        return templates.TemplateResponse(request=request, name="find_influencer.html", context={"ranked_influencers": ranked_influencers, "ad_request": ad_request, "sent_influencer_ids": sent_influencer_ids})
    finally: db.close()


@app.post("/send_campaign_request/{campaign_id}/{ad_request_id}/{influencer_id}", name="send_campaign_request")
async def frontend_send_campaign_request(campaign_id: int, ad_request_id: int, influencer_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor": return redirect_to(request, "login_sponsor")
        form = await request.form()
        campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
        influencer = db.query(User).filter(User.id == influencer_id).first()
        user = frontend_user(request, db)
        if not campaign or not influencer:
            flash(request, "Invalid campaign or influencer.", "danger"); return redirect_to(request, "sponsor_dashboard")
        if not user or campaign.sponsor_id != user.id:
            flash(request, "You do not have permission to send requests for this campaign.", "danger"); return redirect_to(request, "sponsor_dashboard")
        if influencer.role != "influencer" or influencer.is_flagged or campaign.visibility == "archived":
            flash(request, "Invalid influencer or archived campaign.", "danger")
            return redirect_to(request, "sponsor_campaign")
        existing = db.query(AdRequest.id).filter(
            AdRequest.campaign_id == campaign_id,
            AdRequest.influencer_id == influencer_id,
            AdRequest.sponsor_id == user.id
        ).first()
        if existing:
            flash(request, "Request already sent to this influencer for this campaign.", "warning")
            return redirect_to(request, "find_influencer", ad_request_id=ad_request_id)
        ad_request = AdRequest(campaign_id=campaign_id, influencer_id=influencer_id, sponsor_id=user.id, messages=form.get("message"), requirements=campaign.description, payment_amount=campaign.budget, status="new", is_active=True)
        db.add(ad_request); commit_with_events(db); db.refresh(ad_request); clear_analytics_cache()
        flash(request, "Campaign request sent successfully.", "success")
        return redirect_to(request, "sponsor_campaign")
    finally: db.close()


@app.post("/submit_request/{campaign_id}/{influencer_id}", name="submit_request")
async def frontend_submit_request(campaign_id: int, influencer_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor":
            flash(request, "Please log in to access this page", "danger"); return redirect_to(request, "login_sponsor")
        form = await request.form()
        campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
        sponsor_id = request.session["user_id"]
        if not campaign or campaign.sponsor_id != sponsor_id:
            flash(request, "You do not have permission to send a request for this campaign.", "danger"); return redirect_to(request, "sponsor_dashboard_find")
        try:
            parsed_budget = float(form.get("influencer_budget"))
            if parsed_budget <= 0: raise ValueError
        except (ValueError, TypeError):
            flash(request, "Budget must be a valid positive number.", "danger"); return redirect_to(request, "sponsor_dashboard_find")
        new_request = AdRequest(campaign_id=campaign_id, influencer_id=influencer_id, sponsor_id=sponsor_id, requirements=form.get("message"), payment_amount=parsed_budget, status="new", is_active=True)
        db.add(new_request); commit_with_events(db); db.refresh(new_request); clear_analytics_cache()
        flash(request, "Request submitted successfully!", "success")
        return redirect_to(request, "sponsor_dashboard_find")
    finally: db.close()


@app.get("/dummy_payment_gateway", name="dummy_payment_gateway")
def frontend_dummy_payment_gateway(request: Request):
    if request.session.get("role") != "sponsor":
        flash(request, "Please log in to access this page", "danger"); return redirect_to(request, "login_sponsor")
    return templates.TemplateResponse(request=request, name="dummy_payment_gateway.html", context={"request_id": request.query_params.get("request_id"), "amount": request.query_params.get("amount")})


@app.post("/payment_confirmation/{request_id}", name="payment_confirmation")
def frontend_payment_confirmation(request_id: int, request: Request):
    db = SessionLocal()
    try:
        if request.session.get("role") != "sponsor":
            flash(request, "Please log in to access this page", "danger"); return redirect_to(request, "login_sponsor")
        ad_request = db.query(AdRequest).filter(AdRequest.id == request_id).first()
        if not ad_request:
            flash(request, "Ad request not found.", "danger")
        elif ad_request.sponsor_id != request.session.get("user_id"):
            flash(request, "You do not have permission to confirm payment for this ad request.", "danger")
        else:
            change_request_status(ad_request, "paid"); commit_with_events(db); clear_analytics_cache(); flash(request, "Simulated payment recorded (no gateway verification).", "success")
        return redirect_to(request, "sponsor_dashboard")
    finally: db.close()


        
# ============================================================
# KAFKA EVENT HELPER FOR WEB REQUEST CREATION
# ============================================================

def send_kafka_event(ad_request):
    try:
        event = {
            "event_id": str(uuid.uuid4()),
            "event_time": datetime.now(timezone.utc).isoformat(),
            "request_id": ad_request.id,
            "campaign_id": ad_request.campaign_id,
            "influencer_id": ad_request.influencer_id,
            "sponsor_id": ad_request.sponsor_id,
            "payment_amount": ad_request.payment_amount,
            "status": ad_request.status,
        }
        if producer:
            producer.send("ad_requests", key=ad_request.id, value=event)
            producer.flush()
        else:
            print("Kafka unavailable - skipping Kafka event.")
    except Exception as exc:
        print(f"Kafka event warning: {exc}")

# ============================================================
# Redis to cache analytics data for performance
# ============================================================

from sqlalchemy import text


@app.get("/api/analytics/campaigns")
def campaign_analytics(
    current_user: User = Depends(require_role("admin")),
    db: Session = Depends(get_db),
):
    cache_key = "analytics:campaigns"
    cached_data = get_cache(cache_key)
    if cached_data is not None:
        return {"source": "redis", "data": cached_data}

    results = db.execute(text("""
        SELECT c.id AS campaign_id, c.name AS campaign_name,
               COALESCE(c.budget, 0) AS budget,
               COUNT(ar.id) AS request_count,
               COALESCE(SUM(CASE WHEN LOWER(ar.status) = 'paid'
                                 THEN ar.payment_amount ELSE 0 END), 0) AS total_payment
        FROM campaigns c
        LEFT JOIN ad_requests ar ON c.id = ar.campaign_id
        GROUP BY c.id, c.name, c.budget
        ORDER BY COUNT(ar.id) DESC, c.id
    """)).mappings().all()
    data = [
        {"campaign_id": r["campaign_id"], "campaign_name": r["campaign_name"],
         "budget": float(r["budget"] or 0), "request_count": r["request_count"],
         "total_payment": float(r["total_payment"] or 0)}
        for r in results
    ]
    set_cache(cache_key, data, ttl=60)
    return {"source": "postgresql", "data": data}


@app.get("/api/analytics/influencers")
def influencer_analytics(
    current_user: User = Depends(require_role("admin")),
    db: Session = Depends(get_db),
):
    cache_key = "analytics:influencers"
    cached_data = get_cache(cache_key)
    if cached_data is not None:
        return {"source": "redis", "data": cached_data}

    results = db.execute(text("""
        SELECT u.id AS influencer_id, u.username,
               COUNT(ar.id) AS request_count,
               COALESCE(SUM(CASE WHEN LOWER(ar.status) = 'paid'
                                 THEN ar.payment_amount ELSE 0 END), 0) AS total_earnings
        FROM users u
        LEFT JOIN ad_requests ar ON u.id = ar.influencer_id
        WHERE LOWER(u.role) = 'influencer'
        GROUP BY u.id, u.username
        ORDER BY total_earnings DESC, u.id
    """)).mappings().all()
    data = [
        {"influencer_id": r["influencer_id"], "username": r["username"],
         "request_count": r["request_count"],
         "total_earnings": float(r["total_earnings"] or 0)}
        for r in results
    ]
    set_cache(cache_key, data, ttl=60)
    return {"source": "postgresql", "data": data}


@app.get("/api/analytics/admin")
def admin_analytics(
    current_user: User = Depends(require_role("admin")),
    db: Session = Depends(get_db),
):
    cache_key = "analytics:admin"
    cached_data = get_cache(cache_key)
    if cached_data is not None:
        return {"source": "redis", "data": cached_data}

    total_users = db.query(func.count(User.id)).scalar() or 0
    total_influencers = db.query(func.count(User.id)).filter(func.lower(User.role) == "influencer").scalar() or 0
    total_sponsors = db.query(func.count(User.id)).filter(func.lower(User.role) == "sponsor").scalar() or 0
    total_campaigns = db.query(func.count(Campaign.id)).scalar() or 0
    total_requests = db.query(func.count(AdRequest.id)).scalar() or 0
    total_payments = (
        db.query(func.coalesce(func.sum(AdRequest.payment_amount), 0))
        .filter(func.lower(AdRequest.status) == "paid").scalar() or 0
    )
    data = {
        "total_users": total_users,
        "total_influencers": total_influencers,
        "total_sponsors": total_sponsors,
        "total_campaigns": total_campaigns,
        "total_requests": total_requests,
        "total_payments": float(total_payments),
    }
    set_cache(cache_key, data, ttl=60)
    return {"source": "postgresql", "data": data}


# ============================================================
# ROLE-SPECIFIC STATISTICS (LIVE POSTGRESQL + EXISTING REDIS)
# ============================================================
from admin_analytics import get_admin_statistics
from sponsor_analytics import get_sponsor_statistics
from influencer_analytics import get_influencer_statistics

def _cached_statistics(key, calculate):
    cached = get_cache(key)
    if cached is not None:
        return {"source": "redis", "data": cached}
    result = calculate()
    set_cache(key, result, ttl=60)
    return {"source": "postgresql", "data": result}

@app.get("/api/statistics/admin")
def statistics_admin(current_user: User = Depends(require_role("admin")), db: Session = Depends(get_db)):
    return _cached_statistics("analytics:statistics:admin", lambda: get_admin_statistics(db, User, Campaign, AdRequest))

@app.get("/api/statistics/sponsor")
def statistics_sponsor(current_user: User = Depends(require_role("sponsor")), db: Session = Depends(get_db)):
    return _cached_statistics(f"analytics:statistics:sponsor:{current_user.id}",
                              lambda: get_sponsor_statistics(db, Campaign, AdRequest, current_user.id))

@app.get("/api/statistics/influencer")
def statistics_influencer(current_user: User = Depends(require_role("influencer")), db: Session = Depends(get_db)):
    return _cached_statistics(f"analytics:statistics:influencer:{current_user.id}",
                              lambda: get_influencer_statistics(db, Campaign, User, AdRequest, current_user.id))

def _statistics_page(request, role, template_name, calculator):
    db = SessionLocal()
    try:
        user = frontend_user(request, db)
        if not user or user.role != role:
            return redirect_to(request, "login_" + role)
        return templates.TemplateResponse(request=request, name=template_name,
                                          context={"stats":calculator(db, user), "user":user})
    finally:
        db.close()

@app.get("/stats", name="stats")
def admin_statistics_page(request: Request):
    return _statistics_page(request, "admin", "stats.html",
                            lambda db, user: get_admin_statistics(db, User, Campaign, AdRequest))

@app.get("/sponsor_stats", name="sponsor_stats")
def sponsor_statistics_page(request: Request):
    return _statistics_page(request, "sponsor", "sponsor_stats.html",
                            lambda db, user: get_sponsor_statistics(db, Campaign, AdRequest, user.id))

@app.get("/influencer_stats", name="influencer_stats")
def influencer_statistics_page(request: Request):
    return _statistics_page(request, "influencer", "influencer_stats.html",
                            lambda db, user: get_influencer_statistics(db, Campaign, User, AdRequest, user.id))

# ============================================================
# logout
# ============================================================
@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)