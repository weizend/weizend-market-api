import { createServer } from 'node:http';
import {
  WebSocketServer,
  WebSocket
} from 'ws';


// =========================================================
// WEIZEND VERCEL WEBSOCKET RELAY
//
// Tarayıcı:
// wss://weizend-market-api.vercel.app/realtime/ws?user=Weizend
//
//        ↓
//
// Vercel Function
//
//        ↓
//
// wss://weizend-realtime.weizendtv.workers.dev/ws?user=Weizend
// =========================================================

const REALTIME_UPSTREAM =
  'wss://weizend-realtime.weizendtv.workers.dev/ws';


const server = createServer(
  (request, response) => {

    // Normal HTTP isteği gelirse bu endpointin
    // yalnızca WebSocket için olduğunu bildir.

    response.writeHead(
      426,
      {
        'Content-Type':
          'application/json; charset=utf-8',

        'Cache-Control':
          'no-store'
      }
    );

    response.end(
      JSON.stringify({
        success: false,
        error:
          'Bu endpoint WebSocket bağlantısı içindir.'
      })
    );

  }
);


// =========================================================
// WEBSOCKET SERVER
// =========================================================

const wss =
  new WebSocketServer({
    server
  });


// =========================================================
// TARAYICI BAĞLANDI
// =========================================================

wss.on(
  'connection',
  (
    browserSocket,
    request
  ) => {

    let requestUrl;

    try {

      requestUrl =
        new URL(
          request.url || '/',
          'https://weizend-market-api.vercel.app'
        );

    } catch {

      browserSocket.close(
        1008,
        'Invalid URL'
      );

      return;
    }


    const username =
      cleanUsername(
        requestUrl.searchParams.get(
          'user'
        )
      );


    // =====================================================
    // KULLANICI ADI KONTROLÜ
    // =====================================================

    if (
      !isValidUsername(
        username
      )
    ) {

      browserSocket.close(
        1008,
        'Invalid username'
      );

      return;
    }


    // =====================================================
    // CLOUDFLARE REALTIME WORKER'A BAĞLAN
    // =====================================================

    const upstreamUrl =
      `${REALTIME_UPSTREAM}?user=${encodeURIComponent(username)}`;


    const upstream =
      new WebSocket(
        upstreamUrl
      );


    let upstreamOpened =
      false;


    // Cloudflare bağlantısı kurulmadan tarayıcıdan
    // mesaj gelirse kısa süreli kuyruğa al.

    const pendingMessages = [];


    // =====================================================
    // CLOUDFLARE BAĞLANDI
    // =====================================================

    upstream.on(
      'open',
      () => {

        upstreamOpened =
          true;


        while (
          pendingMessages.length >
          0
        ) {

          const message =
            pendingMessages.shift();


          if (
            upstream.readyState ===
            WebSocket.OPEN
          ) {

            upstream.send(
              message.data,
              {
                binary:
                  message.isBinary
              }
            );

          }

        }

      }
    );


    // =====================================================
    // CLOUDFLARE -> TARAYICI
    // =====================================================

    upstream.on(
      'message',
      (
        data,
        isBinary
      ) => {

        if (
          browserSocket.readyState !==
          WebSocket.OPEN
        ) {
          return;
        }


        try {

          browserSocket.send(
            data,
            {
              binary:
                isBinary
            }
          );

        } catch {}

      }
    );


    // =====================================================
    // TARAYICI -> CLOUDFLARE
    // =====================================================

    browserSocket.on(
      'message',
      (
        data,
        isBinary
      ) => {

        if (
          upstream.readyState ===
          WebSocket.OPEN
        ) {

          try {

            upstream.send(
              data,
              {
                binary:
                  isBinary
              }
            );

          } catch {}

          return;
        }


        // Bağlantı henüz açılmadıysa çok büyük
        // bir kuyruk oluşmasına izin verme.

        if (
          !upstreamOpened &&
          pendingMessages.length < 10
        ) {

          pendingMessages.push({
            data,
            isBinary
          });

        }

      }
    );


    // =====================================================
    // CLOUDFLARE BAĞLANTISI KAPANDI
    // =====================================================

    upstream.on(
      'close',
      (
        code,
        reasonBuffer
      ) => {

        if (
          browserSocket.readyState ===
          WebSocket.OPEN
        ) {

          const reason =
            String(
              reasonBuffer || ''
            ).slice(
              0,
              100
            );


          try {

            browserSocket.close(
              validCloseCode(code)
                ? code
                : 1011,

              reason ||
              'Realtime connection closed'
            );

          } catch {}

        }

      }
    );


    // =====================================================
    // CLOUDFLARE HATASI
    // =====================================================

    upstream.on(
      'error',
      error => {

        console.error(
          'UPSTREAM WEBSOCKET ERROR:',
          error
        );


        if (
          browserSocket.readyState ===
          WebSocket.OPEN
        ) {

          try {

            browserSocket.close(
              1011,
              'Realtime upstream error'
            );

          } catch {}

        }

      }
    );


    // =====================================================
    // TARAYICI KAPANDI
    // =====================================================

    browserSocket.on(
      'close',
      () => {

        if (
          upstream.readyState ===
            WebSocket.OPEN ||
          upstream.readyState ===
            WebSocket.CONNECTING
        ) {

          try {

            upstream.close(
              1000,
              'Browser disconnected'
            );

          } catch {}

        }

      }
    );


    // =====================================================
    // TARAYICI HATASI
    // =====================================================

    browserSocket.on(
      'error',
      error => {

        console.error(
          'BROWSER WEBSOCKET ERROR:',
          error
        );


        if (
          upstream.readyState ===
            WebSocket.OPEN ||
          upstream.readyState ===
            WebSocket.CONNECTING
        ) {

          try {
            upstream.terminate();
          } catch {}

        }

      }
    );

  }
);


// =========================================================
// YARDIMCI FONKSİYONLAR
// =========================================================

function cleanUsername(
  value
) {

  return String(
    value || ''
  )
    .trim()
    .replace(
      /^@/,
      ''
    );

}


function isValidUsername(
  value
) {

  return /^[A-Za-z0-9_]{2,40}$/.test(
    String(
      value || ''
    )
  );

}


function validCloseCode(
  code
) {

  return (
    Number.isInteger(code) &&
    code >= 1000 &&
    code <= 4999 &&
    code !== 1004 &&
    code !== 1005 &&
    code !== 1006 &&
    code !== 1015
  );

}


// =========================================================
// VERCEL FUNCTION
// =========================================================

export default server;
