export async function onRequest(context) {
  return context.env.WEIZEND_WORKER.fetch(context.request);
}
