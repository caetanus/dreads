const rhea = require('rhea');
const N = 5;
let sent = 0, acks = 0, got = 0;
const c = rhea.connect({ host: '127.0.0.1', port: 5672, username: 'guest', password: 'guest', idle_time_out: 8000 });
const sender = c.open_sender('rheaq');
sender.on('sendable', () => {
  while (sender.sendable() && sent < N) {
    sender.send({ message_id: 'm' + sent, correlation_id: 'c' + sent, content_type: 'text/plain',
                  application_properties: { idx: sent, tag: 'rhea' }, body: 'payload-' + sent });
    sent++;
  }
});
sender.on('accepted', () => {
  if (++acks === N) {
    console.log('SENT+ACCEPTED', N);
    const receiver = c.open_receiver({ source: 'rheaq', credit_window: 2 });
    receiver.on('message', (ctx) => {
      const m = ctx.message;
      console.log('RECV', m.body, 'ct=' + m.content_type, 'app=' + JSON.stringify(m.application_properties));
      if (++got === N) { console.log('M4 RHEA ROUNDTRIP OK'); c.close(); process.exit(0); }
    });
  }
});
c.on('error', (e) => { console.log('ERR', e); process.exit(1); });
setTimeout(() => { console.log('TIMEOUT sent=%d acks=%d got=%d', sent, acks, got); process.exit(2); }, 15000);
