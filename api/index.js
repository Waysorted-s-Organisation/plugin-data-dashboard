import serverless from "serverless-http";

import app, { initializeServer } from "../src/server.js";

const wrapped = serverless(app);

export default async function handler(req, res) {
  await initializeServer();
  return wrapped(req, res);
}
