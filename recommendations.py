"""
recommendations.py
-------------------
A lightweight, explainable recommendation engine that ranks influencers for
a given ad request / campaign, used by the "Find Influencers" page.

No new database columns are required. It works purely off data the app
already collects:

  1. CONTENT SIMILARITY  (weight 0.5)
     TF-IDF vectorizes the campaign's requirements/description text and
     each influencer's history of past ad-request requirements text, then
     scores them by cosine similarity. This approximates "has this
     influencer worked on similar campaigns before?" without needing an
     explicit 'niche' or 'category' field.

  2. TRACK RECORD  (weight 0.3)
     Historical acceptance rate: accepted_requests / total_requests for
     that influencer, Laplace-smoothed so new influencers with zero
     history aren't scored as 0.

  3. BUDGET FIT  (weight 0.2)
     How close the influencer's average historical deal size is to the
     current campaign's budget. Influencers who don't have a payment
     history get a neutral 0.5 score instead of being penalized.

The three scores are combined into a single 0-1 "match score" per
influencer, which the find_influencer route uses to sort/display results.
"""

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


ACCEPTED_STATUSES = ("accepted", "completed", "paid")


def _influencer_history_text(influencer):
    """Concatenate an influencer's past ad-request requirements into one document."""
    texts = [
        (req.requirements or "")
        for req in influencer.influencer_requests
        if req.requirements
    ]
    return " ".join(texts).strip()


def _content_similarity_scores(query_text, influencers):
    """
    Returns a dict {influencer_id: similarity in [0, 1]} using TF-IDF + cosine
    similarity between the query text (new campaign's requirements) and each
    influencer's aggregated history text. Influencers with no history get 0.
    """
    docs = [query_text] + [_influencer_history_text(inf) for inf in influencers]

    # If nobody (including the query) has usable text, everyone ties at 0.
    if not any(doc.strip() for doc in docs):
        return {inf.id: 0.0 for inf in influencers}

    vectorizer = TfidfVectorizer(stop_words="english")
    try:
        tfidf_matrix = vectorizer.fit_transform(docs)
    except ValueError:
        # Happens if the vocabulary ends up empty (e.g. only stopwords) - fall back to 0s.
        return {inf.id: 0.0 for inf in influencers}

    query_vec = tfidf_matrix[0]
    influencer_vecs = tfidf_matrix[1:]
    sims = cosine_similarity(query_vec, influencer_vecs).flatten()

    return {inf.id: float(sims[i]) for i, inf in enumerate(influencers)}


def _acceptance_rate(influencer):
    """Laplace-smoothed acceptance rate so a brand-new influencer scores ~0.5, not 0."""
    total = len(influencer.influencer_requests)
    accepted = sum(1 for r in influencer.influencer_requests if r.status in ACCEPTED_STATUSES)
    return (accepted + 1) / (total + 2)


def _budget_fit(influencer, campaign_budget, expected_influencers=None):
    """1.0 = past deals matched this budget almost exactly, 0.0 = wildly different, 0.5 = no history."""
    past_payments = [r.payment_amount for r in influencer.influencer_requests if r.payment_amount]
    if not past_payments or not campaign_budget or not expected_influencers:
        return 0.5

    campaign_budget = campaign_budget / expected_influencers
    avg_payment = sum(past_payments) / len(past_payments)
    denom = max(campaign_budget, avg_payment, 1)
    return max(0.0, 1 - abs(campaign_budget - avg_payment) / denom)


def rank_influencers(campaign, influencers, weights=(0.5, 0.3, 0.2)):
    """
    Rank influencers for a given campaign.

    Args:
        campaign: Campaign object (uses .description and .budget)
        influencers: list of User objects with role='influencer'
        weights: (content_weight, track_record_weight, budget_fit_weight)

    Returns:
        List of dicts sorted by match_score desc:
        [{influencer, match_score, content_score, track_record_score, budget_score}, ...]
    """
    w_content, w_track, w_budget = weights
    query_text = campaign.description or "" if campaign else ""

    content_scores = _content_similarity_scores(query_text, influencers)

    ranked = []
    for inf in influencers:
        content_score = content_scores.get(inf.id, 0.0)
        track_score = _acceptance_rate(inf)
        budget_score = _budget_fit(inf, campaign.budget if campaign else None, len(campaign.ad_requests) if campaign and campaign.ad_requests else None)

        match_score = (
            w_content * content_score
            + w_track * track_score
            + w_budget * budget_score
        )

        ranked.append({
            "influencer": inf,
            "match_score": round(match_score * 100, 1),      # as a percentage for display
            "content_score": round(content_score * 100, 1),
            "track_record_score": round(track_score * 100, 1),
            "budget_score": round(budget_score * 100, 1),
        })

    ranked.sort(key=lambda r: r["match_score"], reverse=True)
    return ranked
