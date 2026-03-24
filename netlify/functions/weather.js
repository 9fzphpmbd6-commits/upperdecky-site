// Weather function — fetches from NWS API for ballpark conditions
exports.handler = async (event) => {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers, body: '' };
  }

  const { lat, lon } = event.queryStringParameters || {};
  if (!lat || !lon) {
    return { statusCode: 400, headers, body: JSON.stringify({ error: 'lat and lon required' }) };
  }

  try {
    const pointRes = await fetch(`https://api.weather.gov/points/${lat},${lon}`, {
      headers: { 'User-Agent': 'UpperDecky/2.0 (barrelvision.netlify.app)' }
    });
    const pointData = await pointRes.json();
    const forecastUrl = pointData.properties?.forecast;
    if (!forecastUrl) throw new Error('No forecast URL');

    const fcRes = await fetch(forecastUrl, {
      headers: { 'User-Agent': 'UpperDecky/2.0 (barrelvision.netlify.app)' }
    });
    const fcData = await fcRes.json();
    const current = fcData.properties?.periods?.[0];

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        temperature: current?.temperature,
        unit: current?.temperatureUnit,
        shortForecast: current?.shortForecast,
        windSpeed: current?.windSpeed,
        windDirection: current?.windDirection,
        icon: current?.icon
      })
    };
  } catch (err) {
    return { statusCode: 500, headers, body: JSON.stringify({ error: err.message }) };
  }
};
