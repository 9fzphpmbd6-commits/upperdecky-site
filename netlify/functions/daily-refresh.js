// BarrelVision Daily Data Refresh
// Scheduled function that runs daily to trigger a site rebuild with fresh data
// The actual data generation happens during the build process via a build plugin
// This function triggers a Netlify rebuild hook

const { schedule } = require("@netlify/functions");

const handler = async function(event, context) {
  console.log("[BarrelVision] Daily refresh triggered at", new Date().toISOString());
  
  // If a rebuild hook URL is configured, trigger a rebuild
  const hookUrl = process.env.NETLIFY_REBUILD_HOOK;
  
  if (hookUrl) {
    try {
      const response = await fetch(hookUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ trigger_title: 'Daily data refresh' })
      });
      console.log("[BarrelVision] Rebuild triggered, status:", response.status);
    } catch (err) {
      console.error("[BarrelVision] Failed to trigger rebuild:", err.message);
    }
  } else {
    console.log("[BarrelVision] No NETLIFY_REBUILD_HOOK configured. Set this env var to enable auto-rebuilds.");
  }

  return {
    statusCode: 200,
    body: JSON.stringify({ 
      message: "Daily refresh complete",
      timestamp: new Date().toISOString()
    })
  };
};

// Run daily at 10:00 UTC (6:00 AM ET / 5:00 AM CT)
module.exports.handler = schedule("0 10 * * *", handler);
