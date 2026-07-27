HUBS = {
    "hub_customer": {
        "hk": "customer_hk",
        "bk": "customer_bk",
        "sources": [
            ("crm_customers", "crm_customer_id", "CRM"),
            ("erp_customers", "erp_customer_id", "ERP"),
            ("ecommerce_customers", "ecommerce_customer_id", "ECOMMERCE"),
            ("loyalty_customers", "loyalty_customer_id", "LOYALTY"),
            ("erp_orders", "erp_customer_id", "ERP"),
            ("erp_orders", "customer_id", "ECOMMERCE"),
        ],
    },
    "hub_order": {
        "hk": "order_hk",
        "bk": "order_bk",
        "sources": [
            ("erp_orders", "order_id", "ERP"),
            ("erp_payments", "order_id", "ERP"),
        ],
    },
    "hub_product": {
        "hk": "product_hk",
        "bk": "product_bk",
        "sources": [("ecommerce_products", "product_id", "ECOMMERCE")],
    },
    "hub_seller": {
        "hk": "seller_hk",
        "bk": "seller_bk",
        "sources": [("ecommerce_sellers", "seller_id", "ECOMMERCE")],
    },
}

LINKS = {
    "link_customer_order": {"kind": "customer_order", "source": "erp_orders"},
    "link_order_payment_occurrence": {
        "kind": "order_payment",
        "source": "erp_payments",
    },
}

SATELLITES = {
    "sat_customer_crm": ("crm_customers", "crm_customer_id", "CRM", "customer_hk", []),
    "sat_customer_erp": ("erp_customers", "erp_customer_id", "ERP", "customer_hk", []),
    "sat_customer_ecommerce": (
        "ecommerce_customers",
        "ecommerce_customer_id",
        "ECOMMERCE",
        "customer_hk",
        [],
    ),
    "sat_customer_loyalty": (
        "loyalty_customers",
        "loyalty_customer_id",
        "LOYALTY",
        "customer_hk",
        [],
    ),
    "sat_order_erp": (
        "erp_orders",
        "order_id",
        "ERP",
        "order_hk",
        ["customer_id", "erp_customer_id"],
    ),
    "sat_order_payment_erp": (
        "erp_payments",
        "payment_sequential",
        "PAYMENT_OCCURRENCE",
        "order_payment_hk",
        ["order_id", "payment_id"],
    ),
    "sat_product_ecommerce": (
        "ecommerce_products",
        "product_id",
        "ECOMMERCE",
        "product_hk",
        [],
    ),
    "sat_seller_ecommerce": (
        "ecommerce_sellers",
        "seller_id",
        "ECOMMERCE",
        "seller_hk",
        [],
    ),
}

SAME_AS_SOURCES = [
    ("crm_customers", "crm_customer_id", "CRM", "cpf", "EXACT_CPF"),
    ("erp_customers", "erp_customer_id", "ERP", "cpf", "EXACT_CPF"),
    ("crm_customers", "crm_customer_id", "CRM", "email", "EXACT_EMAIL"),
    (
        "ecommerce_customers",
        "ecommerce_customer_id",
        "ECOMMERCE",
        "email_address",
        "EXACT_EMAIL",
    ),
    ("erp_customers", "erp_customer_id", "ERP", "invoice_email", "EXACT_EMAIL"),
]


