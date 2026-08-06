-- =============================================================================
-- Genie Column Comments — Securities Master Data Lakehouse
-- Gold Layer: statestreet.g_statestreet
--
-- Purpose: These comments enable Databricks Genie AI/BI to answer natural
--          language questions about securities, ratings, coupons, and issuers.
--
-- Run this script once after Gold tables are created, and after any schema change.
-- Safe to re-run (COMMENT ON is idempotent).
-- =============================================================================


-- =============================================================================
-- TABLE: dim_product
-- =============================================================================

COMMENT ON TABLE statestreet.g_statestreet.dim_product IS
  'Security product dimension. One row per active security product across all asset classes. '
  'Covers equities (common stock, preferred stock), debt instruments (bonds, municipal bonds, '
  'pool-backed securities), funds, listed derivatives (options, futures), and subscription rights. '
  'All subtype-specific attributes are flattened into a single row using LEFT JOINs on product_id. '
  'Subtype columns are NULL when they do not apply to the product type. '
  'Example questions: "How many active bonds mature in 2026?" — '
  '"Which equity products were issued after 2020?" — '
  '"Show me all floating-rate bonds with face value above 1000."';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.product_id IS
  'Unique identifier for the security product. Primary key of this table. '
  'Format: alphanumeric string assigned by the source system. '
  'Use this column to join dim_product to fact_product_rating or fact_coupon_schedule.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.id_type IS
  'The type of the primary identifier used to look up this security in external systems. '
  'Allowed values: CUSIP (9-character US/Canada identifier), '
  'ISIN (12-character international standard), '
  'SEDOL (7-character UK identifier), '
  'TICKER (exchange ticker symbol), '
  'BLOOMBERG_ID (Bloomberg proprietary identifier). '
  'Example: a US Treasury bond typically uses CUSIP.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.type IS
  'Top-level asset class of the security. '
  'EQUITY: stocks and equity instruments (common stock, preferred stock). '
  'DEBT: fixed-income instruments (bonds, municipal bonds, pool-backed securities). '
  'FUND: investment funds (mutual funds, ETFs, open-end and closed-end funds). '
  'DERIVATIVE: listed derivatives (options and futures contracts). '
  'RIGHT: subscription rights allowing holders to purchase new shares. '
  'This is the primary filter for most asset-class queries.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.sub_type IS
  'More specific classification within the top-level type. '
  'For EQUITY: COMMON_STOCK or PREFERRED_STOCK. '
  'For DEBT: BOND, MUNI (municipal bond), or POOL_BACKED (asset-backed security). '
  'For FUND: FUND. '
  'For DERIVATIVE: OPTION or FUTURE. '
  'For RIGHT: RIGHT. '
  'May be NULL for older records created before the sub_type field was introduced.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.status IS
  'Current lifecycle status of the security. '
  'ACTIVE: currently tradeable and in good standing. '
  'INACTIVE: no longer actively traded but not formally delisted. '
  'MATURED: the security has reached its maturity date (applies to bonds). '
  'SUSPENDED: trading has been temporarily suspended. '
  'DELISTED: the security has been removed from its exchange listing. '
  'Most analysis should filter to status = ACTIVE unless historical coverage is needed.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.settlement_type IS
  'The settlement method used when this security is traded. '
  'Describes how the transfer of the security and cash are completed after a trade. '
  'Examples: DVP (Delivery vs Payment), FOP (Free of Payment). '
  'May be NULL if the settlement method is not recorded for this security.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.description IS
  'Human-readable name or description of the security. '
  'For example: "Apple Inc Common Stock" or "US Treasury Bond 5.25% 2034". '
  'Useful for search and display. May be NULL for programmatically generated entries.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.issue_date IS
  'The date when this security was originally issued to the market. '
  'Format: DATE (YYYY-MM-DD). '
  'For bonds, this is the dated date of the bond issue. '
  'For equities, this is the IPO date or listing date. '
  'May be NULL if the issue date was not recorded in the source system.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.issue_price IS
  'The price at which this security was originally offered to investors at issuance. '
  'For bonds, this is the price as a percentage of face value (e.g. 100.00 = par). '
  'For equities, this is the IPO price in the face currency. '
  'May be NULL if the issue price was not captured.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.current_face_value IS
  'The current face (par) value of the security. '
  'For bonds and pool-backed securities, this represents the outstanding principal amount '
  'as a percentage of the original face value. A value of 100 means no principal has been repaid. '
  'For equities, this is the nominal or par value per share. '
  'May be NULL for products where face value is not applicable.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.issuer_legal_entity_id IS
  'The identifier of the legal entity (company, government, or institution) that issued this security. '
  'Join to dim_legal_entity on legal_entity_id to get the issuer name, country, and entity type. '
  'May be NULL for securities where the issuer is not recorded.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.tick_ladder_scale_id IS
  'Reference to the tick ladder scale that defines the minimum price increment for this security. '
  'The tick size determines the smallest allowed price movement when trading. '
  'Used primarily for listed equities and derivatives. '
  'May be NULL if tick ladder information is not assigned.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.effective_start_date IS
  'The date from which this version of the product record became active. '
  'Part of the SCD Type 2 (slowly changing dimension) tracking. '
  'When product attributes change, a new row is inserted with a new effective_start_date '
  'and the previous row is closed (effective_end_date is set to the day before).';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.effective_end_date IS
  'The date on which this version of the product record was superseded by a newer version. '
  'A value of 9999-12-31 means this is the currently active version. '
  'Part of the SCD Type 2 (slowly changing dimension) tracking.';

-- ── Equity / Stock columns ────────────────────────────────────────────────────

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.series_id IS
  'For stocks and listed derivatives: the series or share class identifier. '
  'Groups related securities within the same issuance program or class structure. '
  'NULL for non-equity and non-derivative products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.voting_rights IS
  'For common stock only: whether the holder of this share has voting rights '
  'at shareholder meetings. '
  'TRUE = voting shares. FALSE = non-voting shares. '
  'NULL for all non-common-stock products (bonds, funds, derivatives, preferred stock).';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.dividend_type IS
  'For preferred stock only: describes how dividends accumulate or are treated '
  'when not paid. '
  'CUMULATIVE: unpaid dividends accumulate and must be paid before common dividends. '
  'NON_CUMULATIVE: unpaid dividends are forfeited and do not carry forward. '
  'NULL for all non-preferred-stock products.';

-- ── Debt / Bond columns ───────────────────────────────────────────────────────

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.coupon_type IS
  'For bonds only: the type of interest payment structure. '
  'FIXED: the coupon rate stays constant over the life of the bond. '
  'FLOATING: the coupon rate resets periodically based on a reference rate (e.g. LIBOR, SOFR). '
  'ZERO: no periodic coupon payments; the bond is issued at a discount to face value. '
  'NULL for equities, funds, derivatives, and rights. '
  'See fact_coupon_schedule for the actual coupon payment dates and rates.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.maturity_date IS
  'For bonds only: the date on which the bond''s principal is due to be repaid in full. '
  'Format: DATE (YYYY-MM-DD). '
  'After this date, the bond is considered MATURED. '
  'NULL for equities, funds, perpetual bonds, and other non-debt products. '
  'Example question: "Show all bonds maturing between 2025 and 2030."';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.face_currency_code IS
  'For bonds only: the ISO 4217 three-letter currency code in which the bond''s '
  'face value and coupon payments are denominated. '
  'Examples: USD (US Dollar), EUR (Euro), GBP (British Pound), JPY (Japanese Yen). '
  'NULL for equities, funds, and derivatives.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.day_count_convention IS
  'For bonds only: the rule used to calculate the number of days between coupon dates '
  'for interest accrual. '
  'Common values: ACT/360, ACT/365, 30/360, ACT/ACT. '
  'This affects how accrued interest is calculated when a bond trades between coupon dates. '
  'NULL for non-bond products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.tax_exempt IS
  'For municipal bonds (MUNI) only: whether the interest income from this bond is '
  'exempt from federal or state income tax. '
  'TRUE = interest is tax-exempt (typical for US municipal bonds). '
  'FALSE = interest is taxable. '
  'NULL for non-municipal-bond products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.state IS
  'For municipal bonds (MUNI) only: the US state associated with this municipal bond issuance. '
  'Example: CA for California, NY for New York. '
  'NULL for non-municipal products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.purpose IS
  'For municipal bonds (MUNI) only: the stated public purpose for which the bond was issued. '
  'Examples: EDUCATION, TRANSPORTATION, HEALTHCARE, HOUSING. '
  'NULL for non-municipal products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.pool_type IS
  'For pool-backed securities only: the type of underlying asset pool backing this security. '
  'Examples: MORTGAGE (residential or commercial mortgages), AUTO (car loans), '
  'STUDENT_LOAN, CREDIT_CARD. '
  'NULL for non-pool-backed products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.originator IS
  'For pool-backed securities only: the name or identifier of the institution '
  'that originated the underlying loan pool. '
  'NULL for non-pool-backed products.';

-- ── Fund columns ──────────────────────────────────────────────────────────────

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.endness_type IS
  'For funds only: whether the fund has a fixed or unlimited number of shares outstanding. '
  'OPEN_END: new shares are created on demand when investors buy; redeemed when they sell. '
  'CLOSED_END: a fixed number of shares trade on an exchange like a stock. '
  'NULL for all non-fund products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.mutual_fund_type IS
  'For funds only: the specific category or strategy of the mutual fund or ETF. '
  'Examples: EQUITY_FUND, BOND_FUND, BALANCED_FUND, MONEY_MARKET, INDEX_FUND. '
  'NULL for non-fund products.';

-- ── Derivative columns ────────────────────────────────────────────────────────

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.underlying_product_id IS
  'For listed derivatives (options and futures) only: the product_id of the security '
  'that the derivative contract is based on. '
  'Join back to dim_product on product_id to get details about the underlying security. '
  'NULL for non-derivative products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.option_type IS
  'For options only: whether this contract gives the holder the right to buy or sell. '
  'CALL: gives the holder the right to BUY the underlying at the strike price. '
  'PUT: gives the holder the right to SELL the underlying at the strike price. '
  'NULL for all non-option products (stocks, bonds, funds, futures, rights).';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.exercise_style IS
  'For options only: when the option contract can be exercised by the holder. '
  'AMERICAN: can be exercised on any trading day up to and including the expiry date. '
  'EUROPEAN: can only be exercised on the expiry date itself. '
  'NULL for all non-option products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.strike_price IS
  'For options only: the fixed price at which the holder can buy (call) or '
  'sell (put) the underlying security if they choose to exercise the option. '
  'Also called the exercise price. '
  'NULL for all non-option products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.expiry_date IS
  'For options only: the last date on which this option contract can be exercised. '
  'After this date the option expires worthless if not exercised. '
  'Format: DATE (YYYY-MM-DD). '
  'NULL for all non-option products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.delivery_date IS
  'For futures only: the date on which the underlying asset is scheduled to be '
  'delivered to the buyer of the futures contract. '
  'Format: DATE (YYYY-MM-DD). '
  'NULL for all non-futures products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.valuation_method IS
  'For futures only: the method used to calculate the daily profit or loss '
  'on the futures position. '
  'MARK_TO_MARKET: the position is revalued daily against the settlement price. '
  'NULL for all non-futures products.';


-- =============================================================================
-- TABLE: dim_legal_entity
-- =============================================================================

COMMENT ON TABLE statestreet.g_statestreet.dim_legal_entity IS
  'Legal entity dimension. One row per active legal entity in the securities master data. '
  'Legal entities are the organisations that issue securities, act as counterparties, '
  'or serve as custodians and intermediaries. '
  'Join to dim_product using issuer_legal_entity_id = legal_entity_id to identify '
  'the issuer of any security. '
  'SCD Type 2 history is maintained: entity name and attribute changes create new rows '
  'rather than overwriting history. '
  'Example questions: "Which legal entities are based in the United States?" — '
  '"Show all bonds issued by government entities." — '
  '"How many distinct issuers do we have active today?"';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.legal_entity_id IS
  'Unique identifier for the legal entity. Primary key of this table. '
  'Format: alphanumeric string assigned by the source system. '
  'Use this column to join dim_legal_entity to dim_product on issuer_legal_entity_id.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.name IS
  'The full legal name of the entity as registered in the source system. '
  'Examples: "Apple Inc", "US Department of the Treasury", "BlackRock Fund Advisors". '
  'This is the primary display name for identifying the entity in reports.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.country IS
  'The country where the legal entity is domiciled or registered. '
  'Format: ISO 3166-1 alpha-2 two-letter country code. '
  'Examples: US (United States), GB (United Kingdom), DE (Germany), JP (Japan). '
  'Use this column to filter securities by issuer country: '
  '"Show me all bonds issued by entities in Germany."';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.entity_type IS
  'The category or classification of the legal entity. '
  'BANK: commercial bank or financial institution. '
  'CORPORATE: non-financial company (e.g. technology, retail, manufacturing). '
  'GOVERNMENT: sovereign government or government agency. '
  'MUNICIPAL: state, city, or local government entity. '
  'SUPRANATIONAL: international organisation (e.g. World Bank, IMF). '
  'FUND_MANAGER: investment manager or asset management firm. '
  'Useful for filtering issuers by sector: "Show all products issued by government entities."';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.effective_start_date IS
  'The date from which this version of the legal entity record became active. '
  'Part of SCD Type 2 history tracking. When the entity name or attributes change, '
  'a new row is added with a new effective_start_date.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.effective_end_date IS
  'The date on which this version of the legal entity record was superseded. '
  'A value of 9999-12-31 means this is the current, active version of the record. '
  'Filter to effective_end_date = ''9999-12-31'' to see only current entity details.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.is_current IS
  'TRUE if this row represents the current active version of the legal entity. '
  'FALSE if this row has been superseded by a more recent version. '
  'Always filter to is_current = TRUE unless you need historical entity attributes.';


-- =============================================================================
-- TABLE: fact_product_rating
-- =============================================================================

COMMENT ON TABLE statestreet.g_statestreet.fact_product_rating IS
  'Credit rating history fact table. '
  'Grain: one row per security product, per rating agency, per rating date. '
  'Records the credit rating assigned to a security by a recognised rating agency '
  'at a specific point in time. A product may have multiple ratings from different agencies '
  'and those ratings may change over time — each change creates a new row. '
  'Join to dim_product on product_id to add security details (type, issuer, maturity). '
  'Example questions: "Which bonds currently have an AAA rating from S&P?" — '
  '"Show the rating history for product P001." — '
  '"How many products were downgraded from investment grade to junk status last year?"';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.rating_id IS
  'Unique surrogate identifier for this rating record. Primary key of this table. '
  'Not meaningful on its own — use product_id, rating_agency, and rating_date together '
  'to identify a specific rating event.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.product_id IS
  'The security product that was rated. '
  'Join to dim_product on product_id to get the security name, type, and issuer. '
  'Every product_id in this table must exist in dim_product.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.rating_type_id IS
  'Reference to the rating type classification. '
  'Links to the product_rating_type reference table which describes the rating scale '
  'and methodology used by the agency. '
  'May be NULL for older historical rating records.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.rating_value IS
  'The actual credit rating assigned by the agency at rating_date. '
  'Standard S&P / Fitch scale examples (highest to lowest credit quality): '
  'AAA, AA+, AA, AA-, A+, A, A-, BBB+, BBB, BBB- (investment grade); '
  'BB+, BB, BB-, B+, B, B-, CCC+, CCC, CCC-, CC, C, D (below investment grade / default). '
  'Moody''s uses a different notation: Aaa, Aa1, Aa2, A1, Baa1 etc. '
  'BBB- / Baa3 and above is considered "investment grade".';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.rating_date IS
  'The date on which this rating was assigned or last confirmed by the rating agency. '
  'Format: DATE (YYYY-MM-DD). '
  'To find the most recent rating for each product, filter to the MAX(rating_date) '
  'per product_id and rating_agency.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.rating_agency IS
  'The name of the credit rating agency that issued this rating. '
  'SP: Standard and Poors. '
  'MOODYS: Moody''s Investors Service. '
  'FITCH: Fitch Ratings. '
  'Use this column to compare ratings from different agencies for the same product.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.effective_start_date IS
  'The date from which this rating record version was active in the Silver layer. '
  'Part of SCD Type 2 history on the source rating data.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.effective_end_date IS
  'The date on which this rating record was superseded by a newer version. '
  'A value of 9999-12-31 indicates this is the current active rating record.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.is_current IS
  'TRUE if this row is the current active version of the rating record. '
  'Filter to is_current = TRUE to get the latest rating for each product per agency. '
  'FALSE rows represent historical versions kept for audit and trend analysis.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating._dq_rule_version IS
  'Internal audit column. Records the version (SHA256) of the data quality rules '
  'that were applied when this row was evaluated and promoted to Silver/Gold. '
  'Used by the pipeline to identify rows that need to be re-evaluated when DQ rules change. '
  'Not typically needed for business analysis queries.';


-- =============================================================================
-- TABLE: fact_coupon_schedule
-- =============================================================================

COMMENT ON TABLE statestreet.g_statestreet.fact_coupon_schedule IS
  'Coupon payment schedule fact table for bond securities. '
  'Grain: one row per bond per coupon payment date. '
  'Each row represents a single scheduled coupon (interest) payment on a bond. '
  'A bond with semi-annual payments over 10 years will have approximately 20 rows here. '
  'For floating rate bonds, the coupon_rate reflects the rate as of the last reset date. '
  'Join to dim_product on product_id to get bond attributes (maturity date, issuer, currency). '
  'Example questions: "What coupon payments are due in Q1 2026?" — '
  '"Show the full payment schedule for bond P002." — '
  '"Which bonds pay quarterly coupons above 5%?" — '
  '"What is the total annual coupon income from all active fixed-rate bonds?"';

COMMENT ON COLUMN statestreet.g_statestreet.fact_coupon_schedule.coupon_id IS
  'Unique identifier for this coupon payment record. Primary key of this table. '
  'Not meaningful on its own — use product_id and payment_date together to identify '
  'a specific coupon payment event.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_coupon_schedule.product_id IS
  'The bond to which this coupon payment belongs. '
  'Join to dim_product on product_id to get the bond name, maturity date, issuer, and currency. '
  'Every product_id here should exist in dim_product with type = DEBT.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_coupon_schedule.coupon_rate IS
  'The annual coupon interest rate for this payment, expressed as a percentage. '
  'Example: 5.25 means 5.25% per annum. '
  'For FIXED bonds, this rate is constant across all coupon rows for the same bond. '
  'For FLOATING bonds, this rate may differ per payment date as it resets to a reference rate. '
  'For ZERO coupon bonds, this table will have no rows (no periodic payments are made).';

COMMENT ON COLUMN statestreet.g_statestreet.fact_coupon_schedule.payment_date IS
  'The scheduled date on which this coupon payment is made to bondholders. '
  'Format: DATE (YYYY-MM-DD). '
  'Filter to payment_date >= current_date() to see upcoming payments. '
  'Filter to payment_date between two dates to see payments in a specific period. '
  'Example: "Show all coupon payments due in 2025."';

COMMENT ON COLUMN statestreet.g_statestreet.fact_coupon_schedule.coupon_type IS
  'The type of interest structure for this specific coupon payment. '
  'FIXED: the rate is constant and predetermined. '
  'FLOATING: the rate is variable and resets periodically against a benchmark such as SOFR or EURIBOR. '
  'Matches the coupon_type on the parent bond in dim_product.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_coupon_schedule.frequency IS
  'How often coupon payments are made per year for this bond. '
  'ANNUAL: one payment per year. '
  'SEMI_ANNUAL: two payments per year (most common for US corporate and government bonds). '
  'QUARTERLY: four payments per year. '
  'MONTHLY: twelve payments per year (common for mortgage-backed securities). '
  'Example question: "How many bonds pay quarterly coupons?"';

COMMENT ON COLUMN statestreet.g_statestreet.fact_coupon_schedule._dq_rule_version IS
  'Internal audit column. Records the version (SHA256) of the data quality rules '
  'applied when this row was evaluated and promoted to the Gold layer. '
  'Not typically used in business queries. Useful for pipeline operations and audits.';

---
