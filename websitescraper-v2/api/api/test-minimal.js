export default async function handler(req, res) {
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }
  
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  // Just return a fake job_id immediately
  return res.status(200).json({
    status: 'queued',
    job_id: `scr_test_${Date.now()}`,
    eta_ms: 15000,
    note: 'Minimal test endpoint'
  });
}