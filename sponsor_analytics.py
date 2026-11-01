from sqlalchemy import func, case
from sqlalchemy.orm import Session

def count_status(db, AdRequest, status, owner_field=None, owner_id=None):
    q = db.query(func.count(AdRequest.id)).filter(func.lower(AdRequest.status) == status)
    if owner_field is not None:
        q = q.filter(owner_field == owner_id)
    return q.scalar() or 0

def get_sponsor_statistics(db: Session, Campaign, AdRequest, sponsor_id: int):
    campaigns = db.query(Campaign).filter(Campaign.sponsor_id == sponsor_id).all()
    req = db.query(AdRequest).filter(AdRequest.sponsor_id == sponsor_id)
    total = req.count()
    paid = count_status(db, AdRequest, "paid", AdRequest.sponsor_id, sponsor_id)
    completed = count_status(db, AdRequest, "completed", AdRequest.sponsor_id, sponsor_id)
    accepted = count_status(db, AdRequest, "accepted", AdRequest.sponsor_id, sponsor_id)
    pending = count_status(db, AdRequest, "pending", AdRequest.sponsor_id, sponsor_id)
    revenue = req.filter(func.lower(AdRequest.status) == "paid").with_entities(func.coalesce(func.sum(AdRequest.payment_amount),0)).scalar() or 0
    rows=[]
    for c in campaigns:
        cq = db.query(AdRequest).filter(AdRequest.campaign_id == c.id, AdRequest.sponsor_id == sponsor_id)
        n=cq.count()
        amount=cq.filter(func.lower(AdRequest.status)=="paid").with_entities(func.coalesce(func.sum(AdRequest.payment_amount),0)).scalar() or 0
        rows.append({"campaign_id":c.id,"campaign_name":c.name,"budget":float(c.budget or 0),
                     "total_requests":n,"paid_amount":float(amount),
                     "budget_utilization_pct":round(float(amount)*100/c.budget,2) if c.budget else 0})
    statuses=req.with_entities(AdRequest.status,func.count(AdRequest.id)).group_by(AdRequest.status).all()
    return {"total_campaigns":len(campaigns),"total_requests":total,"accepted_requests":accepted,
            "pending_requests":pending,"completed_requests":completed,"paid_requests":paid,
            "successful_requests":completed+paid,"success_rate":round((completed+paid)*100/total,2) if total else 0,
            "total_paid":float(revenue),"campaigns":rows,
            "status_breakdown":[{"status":s or "unknown","count":n} for s,n in statuses]}
