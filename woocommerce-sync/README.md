# WooCommerce-Sync fuer Lager-MC

Eigenstaendiger Python-Worker mit PostgreSQL-Outbox, Datenbanktransport nach
MySQL/MariaDB und mitgelieferter WordPress-Bruecke. Keine Shop-HTTP-Aufrufe.

Einrichtung, Rechte, Betrieb und Funktionsgrenzen:
[docs/woocommerce-sync.md](../docs/woocommerce-sync.md).

```bash
python woocommerce-sync/woocommerce_sync.py --help
```

Docker wird vom Repository-Stamm gebaut:
`docker build -f woocommerce-sync/Dockerfile -t lager-woo-sync .`.
