#!/usr/bin/env bash
# Demo lengkap. Jalankan dari root repo setelah: docker compose up -d --build
set -u
A=http://localhost:8000; B=http://localhost:8001
post() { curl -s -w "\n  -> HTTP %{http_code}\n" -X POST "$1" -H 'Content-Type: application/json' -d "$2"; }
step() { echo; echo "=== $1 ==="; }

step "1. Checkout normal (sinkron OK + event asinkron)"
post $A/checkout '{"customer":"Sari","email":"sari@mail.com","items":[{"sku":"A1","qty":2}]}'
sleep 2; curl -s $B/report; echo

step "2. Status HTTP berbeda: SKU salah (404), stok kurang (409), bayar ditolak (402)"
post $A/checkout '{"customer":"Sari","email":"s@m.com","items":[{"sku":"ZZ","qty":1}]}'
post $A/checkout '{"customer":"Sari","email":"s@m.com","items":[{"sku":"C3","qty":999}]}'
post $A/checkout '{"customer":"Sari","email":"s@m.com","items":[{"sku":"C3","qty":20},{"sku":"A1","qty":90},{"sku":"B2","qty":500}]}'

step "3. Layanan B DIBEKUKAN (pause) -> sinkron timeout 504"
docker compose pause service-b
time post $A/checkout '{"customer":"Budi","email":"budi@mail.com","items":[{"sku":"A1","qty":1}]}'
docker compose unpause service-b

step "4. Layanan B MATI (stop) -> sinkron 503"
docker compose stop service-b
post $A/checkout '{"customer":"Budi","email":"budi@mail.com","items":[{"sku":"A1","qty":1}]}'

step "5. Asinkron saat B mati: batalkan order 1 -> tetap 200, event antre di broker"
post $A/orders/1/cancel ''
echo "  Antrean di RabbitMQ:"; docker compose exec rabbitmq rabbitmqctl list_queues name messages

step "6. B dihidupkan lagi -> pesan yang antre diproses, stok dipulihkan"
docker compose start service-b
sleep 8; curl -s $B/report; echo
