from sqlalchemy import func, case
from sqlalchemy.orm import Session

def count_status(db, AdRequest, status, owner_field=None, owner_id=None):
    q = db.query(func.count(AdRequest.id)).filter(func.lower(AdRequest.status) == status)
    if owner_field is not None:
        q = q.filter(owner_field == owner_id)
    return q.scalar() or 0

def get_admin_statistics(db: Session, User, Campaign, AdRequest):
    users = db.query(User).count()
    sponsors = db.query(User).filter(func.lower(User.role) == "sponsor").count()
    influencers = db.query(User).filter(func.lower(User.role) == "influencer").count()
    campaigns = db.query(Campaign).count()
    requests = db.query(AdRequest).count()
    paid = count_status(db, AdRequest, "paid")
    completed = count_status(db, AdRequest, "completed")
    pending = count_status(db, AdRequest, "pending")
    accepted = count_status(db, AdRequest, "accepted")
    rejected = count_status(db, AdRequest, "rejected")
    revenue = db.query(func.coalesce(func.sum(AdRequest.payment_amount), 0)).filter(func.lower(AdRequest.status) == "paid").scalar() or 0
    statuses = db.query(AdRequest.status, func.count(AdRequest.id)).group_by(AdRequest.status).order_by(AdRequest.status).all()
    return {"total_users":users, "total_sponsors":sponsors, "total_influencers":influencers,
            "total_campaigns":campaigns, "total_requests":requests, "paid_requests":paid,
            "completed_requests":completed, "pending_requests":pending,
            "accepted_requests":accepted, "rejected_requests":rejected,
            "successful_requests":paid+completed, "success_rate":round((paid+completed)*100/requests,2) if requests else 0,
            "total_payments":float(revenue), "status_breakdown":[{"status":s or "unknown","count":n} for s,n in statuses]}
