#!/usr/bin/env bash
# ===========================================================================
# Anu LCBO Tracker — One-shot Fly.io deploy
# ===========================================================================
#
# Run this ONCE after `flyctl auth login`. It:
#   1. Creates the Fly app (if not exists)
#   2. Asks for the env vars (copy from your Render dashboard)
#   3. Deploys the Docker image
#   4. Prints the new public URL — paste that into Vercel as NEXT_PUBLIC_API_BASE
#
# Cost: ~$3-5/mo on shared-cpu-1x with 512MB RAM (free $5/mo trial credit covers
# the first month). No surprise charges.
#
# Why Fly.io over Render:
#   - More reliable under load (your Render instance died from a 200-parallel burst)
#   - No "credits used" emails every 15 min during deploys
#   - Toronto region (yyz) — same continent as Neon
#
# Prereqs:
#   curl -L https://fly.io/install.sh | sh
#   export PATH="$HOME/.fly/bin:$PATH"
#   flyctl auth login
# ===========================================================================
set -e

export PATH="$HOME/.fly/bin:$PATH"

if ! command -v flyctl >/dev/null 2>&1; then
  echo "❌ flyctl not in PATH. Run:"
  echo "   curl -L https://fly.io/install.sh | sh"
  echo "   export PATH=\"\$HOME/.fly/bin:\$PATH\""
  exit 1
fi

if ! flyctl auth whoami >/dev/null 2>&1; then
  echo "❌ Not logged in. Run: flyctl auth login"
  echo "   (opens a browser — use your GitHub or email)"
  exit 1
fi

USER=$(flyctl auth whoami 2>&1 | head -1)
echo "✓ Logged in as: $USER"
echo ""

APP_NAME="${APP_NAME:-lcbo-tracker}"
REGION="${REGION:-yyz}"  # Toronto

# Create the app if it doesn't exist
if flyctl status -a "$APP_NAME" >/dev/null 2>&1; then
  echo "✓ App '$APP_NAME' already exists, skipping launch"
else
  echo "→ Creating app '$APP_NAME' in region $REGION..."
  flyctl launch --copy-config --no-deploy --yes --name "$APP_NAME" --region "$REGION" --org personal
fi

echo ""
echo "→ Setting env vars from .env.fly (if present)"
if [ -f .env.fly ]; then
  # Read each line and set as a Fly secret
  while IFS='=' read -r key value; do
    [[ "$key" =~ ^[[:space:]]*# ]] && continue
    [ -z "$key" ] && continue
    # Strip surrounding quotes from value if any
    value="${value%\"}"
    value="${value#\"}"
    echo "   $key"
    flyctl secrets set "${key}=${value}" -a "$APP_NAME" --stage
  done < .env.fly
  echo "→ Deploying secrets..."
  flyctl secrets deploy -a "$APP_NAME" 2>/dev/null || true
else
  echo "⚠ No .env.fly file found. Set secrets manually:"
  echo "   flyctl secrets set DATABASE_URL=... -a $APP_NAME"
  echo "   flyctl secrets set SOD_USER=... SOD_PASSWORD=... SOD_AGENT_ID=1113 -a $APP_NAME"
  echo "   flyctl secrets set SOD_CRON_TOKEN=... ANTHROPIC_API_KEY=... -a $APP_NAME"
  echo "   flyctl secrets set RESEND_API_KEY=... ALERT_EMAIL_TO=ikshit@anuspirits.com -a $APP_NAME"
  echo "   flyctl secrets set ALERT_EMAIL_FROM=alerts@anuspirits.com -a $APP_NAME"
  echo "   flyctl secrets set TASTING_DIGEST_TO=ikshit@anuspirits.com,sales@anuspirits.com -a $APP_NAME"
  echo "   flyctl secrets set ADMIN_TOKEN=\$(openssl rand -hex 32) -a $APP_NAME"
  echo "   flyctl secrets set CORS_ORIGINS=https://lcbo-tracker-web.vercel.app -a $APP_NAME"
fi

echo ""
echo "→ Deploying app..."
flyctl deploy -a "$APP_NAME" --remote-only

echo ""
echo "============================================================"
echo "✅ DEPLOYED"
echo "============================================================"

URL="https://${APP_NAME}.fly.dev"
echo "Public URL:  $URL"
echo ""
echo "→ Verify health:"
echo "   curl $URL/healthz"
echo ""
echo "→ Verify the new endpoints (cities/anu-import/route-planner):"
echo "   curl $URL/api/crm/cities | head -c 200"
echo ""
echo "→ Test alerts (replace TOKEN with your ADMIN_TOKEN):"
echo "   curl -X POST -H 'X-Admin-Token: TOKEN' '$URL/api/admin/test-alert?subject=Fly+is+live'"
echo ""
echo "→ Trigger a backup-to-email right now:"
echo "   curl -X POST -H 'X-Admin-Token: TOKEN' $URL/api/admin/run-backup-now"
echo ""
echo "============================================================"
echo "🔗 NEXT STEP — point Vercel at the new backend"
echo "============================================================"
echo ""
echo "  1. https://vercel.com/dashboard → lcbo-tracker-web → Settings → Env Variables"
echo "  2. Edit NEXT_PUBLIC_API_BASE for Production:"
echo "       OLD: https://lcbo-tracker.onrender.com"
echo "       NEW: $URL"
echo "  3. Deployments → click ⋯ on latest → Redeploy (without cache)"
echo "  4. Wait ~90s, visit https://lcbo-tracker-web.vercel.app — now hitting Fly"
echo ""
echo "  Once verified, you can suspend the Render service to stop the credit emails:"
echo "  Render dashboard → lcbo-tracker → Settings → Suspend Service"
echo ""
