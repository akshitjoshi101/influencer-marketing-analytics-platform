from sqlalchemy import func, case
from sqlalchemy.orm import Session

def count_status(db, AdRequest, status, owner_field=None, owner_id=None):
    q = db.query(func.count(AdRequest.id)).filter(func.lower(AdRequest.status) == status)
    if owner_field is not None:
        q = q.filter(owner_field == owner_id)
    return q.scalar() or 0

def get_influencer_statistics(db: Session, Campaign, User, AdRequest, influencer_id: int):
    req=db.query(AdRequest).filter(AdRequest.influencer_id == influencer_id)
    total=req.count()
    paid=count_status(db,AdRequest,"paid",AdRequest.influencer_id,influencer_id)
    completed=count_status(db,AdRequest,"completed",AdRequest.influencer_id,influencer_id)
    accepted=count_status(db,AdRequest,"accepted",AdRequest.influencer_id,influencer_id)
    pending=count_status(db,AdRequest,"pending",AdRequest.influencer_id,influencer_id)
    earned=req.filter(func.lower(AdRequest.status)=="paid").with_entities(func.coalesce(func.sum(AdRequest.payment_amount),0)).scalar() or 0
    rows=(db.query(AdRequest.id,Campaign.name,User.username,AdRequest.status,AdRequest.payment_amount)
          .join(Campaign,AdRequest.campaign_id==Campaign.id)
          .join(User,AdRequest.sponsor_id==User.id)
          .filter(AdRequest.influencer_id==influencer_id).order_by(AdRequest.id.desc()).all())
    statuses=req.with_entities(AdRequest.status,func.count(AdRequest.id)).group_by(AdRequest.status).all()
    return {"total_requests":total,"accepted_requests":accepted,"pending_requests":pending,
            "completed_requests":completed,"paid_requests":paid,"successful_deals":paid+completed,
            "success_rate":round((paid+completed)*100/total,2) if total else 0,
            "total_earnings":float(earned),
            "collaborations":[{"request_id":rid,"campaign_name":name,"sponsor":sponsor,
                               "status":status,"payment_amount":float(amount or 0)}
                              for rid,name,sponsor,status,amount in rows],
            "status_breakdown":[{"status":s or "unknown","count":n} for s,n in statuses]}
