-- =============================================================================
-- Genie Column Comments — Securities Master Data Lakehouse
-- Gold Layer: statestreet.g_statestreet
--
-- Purpose: Enables Databricks Genie AI/BI to answer natural language questions
--          about securities, ratings, coupons, and legal entities.
--
-- Run this notebook once after the Gold layer is built (notebook 05_gold_build.sql).
-- Safe to re-run — COMMENT ON is idempotent.
-- =============================================================================

-- =============================================================================
-- dim_product
-- =============================================================================

COMMENT ON TABLE statestreet.g_statestreet.dim_product IS
  'Securities product dimension. One row per security product (current version only). '
  'Covers all product types: equities (common stock, preferred stock), '
  'debt instruments (bonds, municipal bonds, pool-backed securities), '
  'funds (mutual funds, ETFs), listed derivatives (options, futures), and rights. '
  'Subtype-specific attributes are flattened into a single wide row — '
  'columns that do not apply to a given product type will be NULL. '
  'Join to fact tables on product_id. '
  'Source: 10 Silver tables joined on product_id (product, stock, common_stock, '
  'preferred_stock, debt, bond, muni, fund, listed_derivative, option, future).';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.product_id IS
  'Unique identifier for the security. Primary key. '
  'Shared across all product subtype tables — use this column to join '
  'dim_product to fact_product_rating and fact_coupon_schedule.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.id_type IS
  'The type of the primary identifier stored in product_id. '
  'Values: CUSIP (9-character US identifier), ISIN (12-character international), '
  'SEDOL (7-character UK), TICKER (exchange ticker symbol), '
  'BLOOMBERG_ID (Bloomberg terminal identifier).';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.type IS
  'Top-level product category. '
  'EQUITY = publicly traded stocks and shares. '
  'DEBT = bonds, municipal bonds, and asset-backed securities. '
  'FUND = mutual funds, ETFs, and closed-end funds. '
  'DERIVATIVE = listed options and futures contracts. '
  'RIGHT = subscription rights issued to existing shareholders. '
  'Filter on this column first to narrow your search by asset class.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.sub_type IS
  'More specific product subcategory within the top-level type. '
  'Equity subtypes: COMMON_STOCK, PREFERRED_STOCK. '
  'Debt subtypes: BOND, MUNI (municipal bond), POOL_BACKED (asset-backed security). '
  'Derivative subtypes: OPTION, FUTURE. '
  'Other: FUND, RIGHT. '
  'NULL for older records predating the sub_type field introduction.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.status IS
  'Current lifecycle status of the security. '
  'ACTIVE = currently tradeable. '
  'INACTIVE = not currently trading but not formally delisted. '
  'MATURED = bond or note that has reached its maturity date and been repaid. '
  'SUSPENDED = temporarily halted from trading. '
  'DELISTED = permanently removed from the exchange. '
  'Most analyses should filter to status = ''ACTIVE''.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.description IS
  'Human-readable name or description of the security. '
  'Examples: "Apple Inc Common Stock", "US Treasury Bond 5yr 3.5%", '
  '"Vanguard S&P 500 ETF". Useful for searching by company or instrument name.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.issue_date IS
  'Date when the security was first issued or listed. '
  'For bonds, this is the original issuance date — used with maturity_date '
  'to calculate remaining term. NULL if the issue date is not recorded.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.issue_price IS
  'Price at which the security was originally issued. '
  'For bonds, this is typically 100 (par). '
  'For equities, this is the IPO or listing price. '
  'NULL if not recorded. Stored in the currency of the face_currency_code.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.current_face_value IS
  'Current face or par value of the security, expressed as a percentage (0–100). '
  'For bonds, this typically starts at 100 and may decrease as principal is repaid. '
  'For pool-backed securities, this reflects the remaining pool balance percentage. '
  'NULL for equities, funds, and derivatives.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.issuer_legal_entity_id IS
  'Identifier of the legal entity that issued this security. '
  'Links to dim_legal_entity.legal_entity_id for issuer name, country, and type. '
  'NULL for securities where the issuer is not recorded in our reference data.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.effective_start_date IS
  'Date when this version of the product record became active. '
  'Part of SCD2 (Slowly Changing Dimension Type 2) tracking. '
  'Use this with effective_end_date to see what the record looked like '
  'at any point in time. All rows in this table have is_current = TRUE.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.effective_end_date IS
  'Date when this version of the product record was superseded. '
  'All current rows have effective_end_date = 9999-12-31 (open-ended). '
  'This table only contains current rows (is_current = TRUE).';

-- Equity-specific columns
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.series_id IS
  'Series identifier for stocks and listed derivatives that belong to a named series. '
  'Links to the series reference table. NULL for bonds, funds, and rights.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.voting_rights IS
  'Whether the stock carries voting rights at shareholder meetings. '
  'TRUE = voting rights, FALSE = no voting rights. '
  'Applies to common stock only. NULL for all other product types.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.dividend_type IS
  'Dividend payment structure for preferred stock. '
  'CUMULATIVE = unpaid dividends accumulate and must be paid before common dividends. '
  'NON_CUMULATIVE = unpaid dividends do not accumulate. '
  'NULL for common stock, bonds, funds, and derivatives.';

-- Debt/Bond-specific columns
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.coupon_type IS
  'Interest payment structure for debt securities. '
  'FIXED = constant interest rate throughout the bond''s life. '
  'FLOATING = interest rate resets periodically based on a benchmark (e.g. SOFR, LIBOR). '
  'ZERO = no periodic payments; bond is issued at a discount and repaid at par. '
  'STEP_UP = rate increases at predetermined intervals. '
  'NULL for equities, funds, and derivatives. '
  'For detailed coupon payment dates and rates, see fact_coupon_schedule.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.maturity_date IS
  'Date when the bond or debt instrument is scheduled to repay its principal. '
  'After this date, the bond ceases to exist and status becomes MATURED. '
  'Use YEAR(maturity_date) to find bonds maturing in a specific year. '
  'NULL for equities, funds, perpetual bonds, and derivatives.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.face_currency_code IS
  'ISO 4217 three-letter currency code for the bond''s face/par value. '
  'Examples: USD (US dollar), EUR (euro), GBP (British pound), JPY (Japanese yen). '
  'Indicates the currency in which principal and coupon payments are made. '
  'NULL for equities, funds, and derivatives.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.day_count_convention IS
  'Day count method used to calculate accrued interest between coupon payments. '
  'Common values: ACT/360 (actual days over 360), ACT/365 (actual days over 365), '
  '30/360 (each month treated as 30 days). '
  'Relevant for bond pricing and settlement calculations. '
  'NULL for non-debt products.';

-- Municipal bond-specific columns
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.tax_exempt IS
  'Whether interest payments on this municipal bond are exempt from federal income tax. '
  'TRUE = tax-exempt (most US municipal bonds). FALSE = taxable municipal bond. '
  'NULL for non-municipal-bond products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.muni_state IS
  'US state that issued this municipal bond. '
  'Two-letter state abbreviation (e.g. CA, NY, TX). '
  'NULL for non-municipal-bond products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.muni_purpose IS
  'Purpose for which this municipal bond was issued. '
  'Examples: GENERAL_OBLIGATION, REVENUE, EDUCATION, TRANSPORTATION, HOUSING. '
  'NULL for non-municipal-bond products.';

-- Pool-backed security columns
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.pool_type IS
  'Type of asset pool backing this security. '
  'Examples: MORTGAGE, AUTO_LOAN, STUDENT_LOAN, CREDIT_CARD. '
  'Applies to pool-backed securities (MBS, ABS) only. NULL for all other types.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.originator IS
  'Financial institution that originated the underlying loans in the pool. '
  'Examples: bank name or mortgage company. '
  'Applies to pool-backed securities only. NULL for all other types.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.total_amount_issued IS
  'Total principal amount issued for this debt security. '
  'Represents the original face value of the debt offering. '
  'NULL for non-debt products.';

-- Fund-specific columns
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.endness_type IS
  'Whether the fund is open-ended or closed-ended. '
  'OPEN_END = new shares can be created or redeemed daily at NAV (e.g. mutual funds, ETFs). '
  'CLOSED_END = fixed number of shares trade on exchange at market price. '
  'NULL for non-fund products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.mutual_fund_type IS
  'Subcategory of the fund. '
  'Examples: EQUITY_FUND, BOND_FUND, MONEY_MARKET, BALANCED, INDEX. '
  'NULL for non-fund products.';

-- Derivative-specific columns
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.underlying_product_id IS
  'Product identifier of the security that this derivative is based on. '
  'For options and futures, this points to the underlying stock, bond, or index. '
  'Join back to dim_product on this column to get underlying security details. '
  'NULL for non-derivative products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.option_type IS
  'Whether this option gives the right to buy or sell the underlying security. '
  'CALL = right to buy the underlying at the strike price. '
  'PUT = right to sell the underlying at the strike price. '
  'NULL for non-option products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.exercise_style IS
  'When the option can be exercised. '
  'AMERICAN = can be exercised on any trading day up to and including expiry. '
  'EUROPEAN = can only be exercised on the expiry date itself. '
  'NULL for non-option products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.strike_price IS
  'The fixed price at which an option holder can buy (call) or sell (put) '
  'the underlying security. Also called the exercise price. '
  'NULL for non-option products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.expiry_date IS
  'Date on which the option contract expires and becomes worthless if unexercised. '
  'NULL for non-option products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.delivery_date IS
  'Date on which the futures contract settles and the underlying asset '
  'is delivered (physical settlement) or the contract is cash-settled. '
  'NULL for non-futures products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.valuation_method IS
  'How this futures contract is valued and settled daily. '
  'MARK_TO_MARKET = daily settlement at closing price against prior day. '
  'NULL for non-futures products.';

-- =============================================================================
-- dim_legal_entity
-- =============================================================================

COMMENT ON TABLE statestreet.g_statestreet.dim_legal_entity IS
  'Legal entity dimension. One row per active legal entity. '
  'Legal entities are the issuers, counterparties, and custodians '
  'associated with securities in the portfolio. '
  'Join to dim_product on issuer_legal_entity_id = legal_entity_id '
  'to see all securities issued by a given entity. '
  'Source: Silver s_statestreet.legal_entity (current rows only).';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.legal_entity_id IS
  'Unique identifier for the legal entity. Primary key. '
  'Referenced by dim_product.issuer_legal_entity_id. '
  'Use this to find all securities issued by a specific entity.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.legal_name IS
  'Full registered legal name of the entity. '
  'Examples: "Apple Inc", "US Department of the Treasury", '
  '"JPMorgan Chase Bank NA", "State of California". '
  'Use this column when searching by company or government name.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.country IS
  'ISO 3166-1 alpha-2 two-letter country code where the entity is domiciled. '
  'Examples: US (United States), GB (United Kingdom), DE (Germany), JP (Japan). '
  'Use this to filter by issuer country or analyse geographic exposure.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.entity_type IS
  'Classification of the legal entity by organisation type. '
  'BANK = commercial or investment bank. '
  'CORPORATE = non-financial company. '
  'GOVERNMENT = sovereign, federal, or national government. '
  'MUNICIPAL = city, county, or state/provincial government. '
  'SUPRANATIONAL = international organisation (e.g. World Bank, EIB). '
  'NULL for entities where the type has not been classified.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.effective_start_date IS
  'Date when this version of the legal entity record became active. '
  'Part of SCD2 history tracking. All rows in this table have is_current = TRUE.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.effective_end_date IS
  'Date when this version of the legal entity record was superseded. '
  'All current rows have effective_end_date = 9999-12-31.';

-- =============================================================================
-- fact_product_rating
-- =============================================================================

COMMENT ON TABLE statestreet.g_statestreet.fact_product_rating IS
  'Credit rating history for securities. '
  'Grain: one row per product per rating agency per rating date. '
  'A single security may have multiple rows if rated by several agencies '
  '(e.g. S&P, Moody''s, and Fitch) or if its rating has changed over time. '
  'To get the most recent rating, filter on is_current_rating = TRUE. '
  'Join to dim_product on product_id for security attributes. '
  'Source: Silver s_statestreet.product_rating joined to product_rating_type.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.product_rating_id IS
  'Unique identifier for this rating record. Primary key of this table.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.product_id IS
  'Identifier of the security being rated. '
  'Join to dim_product.product_id to get full security details.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.product_rating_type_id IS
  'Identifier of the rating methodology or scale being applied. '
  'Different rating agencies use different scales. '
  'Join to the product_rating_type reference for scale details.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.rating_value IS
  'The credit rating assigned to the security. '
  'Investment grade: AAA (highest), AA+, AA, AA-, A+, A, A-, BBB+, BBB, BBB-. '
  'Sub-investment grade (high yield): BB+, BB, BB-, B+, B, B-, CCC, CC, C. '
  'Default: D. '
  'Not rated: NR. '
  'Note: Moody''s uses a different notation (Aaa, Aa1, Aa2, etc.).';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.effective_from_date IS
  'Date when this rating became effective. '
  'Use this column to filter ratings as of a specific date or '
  'to track rating changes over time for a given security.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.rating_agency IS
  'Name of the credit rating agency that assigned this rating. '
  'SP = Standard & Poor''s. '
  'MOODYS = Moody''s Investors Service. '
  'FITCH = Fitch Ratings.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.watch_code IS
  'Rating watch or outlook assigned alongside the rating. '
  'POSITIVE = rating may be upgraded. '
  'NEGATIVE = rating may be downgraded. '
  'STABLE = rating unlikely to change in the near term. '
  'DEVELOPING = rating could move in either direction (e.g. during M&A). '
  'NULL if no watch or outlook has been assigned.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.rating_scale IS
  'The scale or universe this rating applies to. '
  'LONG_TERM = rating for obligations with maturity greater than one year. '
  'SHORT_TERM = rating for obligations with maturity of one year or less. '
  'NULL if not specified.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.rating_type_code IS
  'Code identifying the specific rating type within the agency''s methodology. '
  'Examples: ISSUER, ISSUE, SENIOR_UNSECURED, SUBORDINATED. '
  'Distinguishes between ratings on the issuer itself versus specific debt issues.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.product_type IS
  'Product type of the rated security at the time of rating. '
  'Denormalised from dim_product for convenience. '
  'Values: EQUITY, DEBT, FUND, DERIVATIVE, RIGHT.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.product_status IS
  'Lifecycle status of the rated security at the time of rating. '
  'Denormalised from dim_product for convenience. '
  'Values: ACTIVE, INACTIVE, MATURED, SUSPENDED, DELISTED.';

-- =============================================================================
-- fact_coupon_schedule
-- =============================================================================

COMMENT ON TABLE statestreet.g_statestreet.fact_coupon_schedule IS
  'Coupon payment schedule for bond and debt securities. '
  'Grain: one row per bond per coupon payment date. '
  'Each row represents a single scheduled interest payment on a debt instrument. '
  'A 10-year semi-annual bond will have approximately 20 rows in this table. '
  'Join to dim_product on product_id to get the bond''s full details '
  '(maturity date, face currency, coupon type, issuer, etc.). '
  'Source: Silver s_statestreet.coupon joined to s_statestreet.bond.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_coupon_schedule.coupon_id IS
  'Unique identifier for this coupon payment record. Primary key of this table.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_coupon_schedule.product_id IS
  'Identifier of the bond making this coupon payment. '
  'Join to dim_product.product_id to get bond attributes such as '
  'issuer, maturity date, face value, and currency.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_coupon_schedule.payment_date IS
  'Scheduled date on which the coupon interest payment is made to bondholders. '
  'Use this to find upcoming payments (payment_date >= current_date()) '
  'or to summarise payments within a date range.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_coupon_schedule.coupon_rate IS
  'Annual interest rate for this coupon period, expressed as a decimal. '
  'Example: 0.05 = 5% annual coupon rate. '
  'For FIXED rate bonds this is constant across all rows for the same product. '
  'For FLOATING rate bonds this changes each period based on the benchmark rate reset. '
  'Multiply by face value to calculate the payment amount.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_coupon_schedule.coupon_type IS
  'Interest rate structure for this specific coupon payment. '
  'FIXED = rate is set at issuance and does not change. '
  'FLOATING = rate resets each period against a benchmark index (e.g. SOFR + spread). '
  'Denormalised from the bond record for analytical convenience.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_coupon_schedule.frequency IS
  'How often coupon payments are made per year. '
  'ANNUAL = once per year. '
  'SEMI_ANNUAL = twice per year (most common for US bonds). '
  'QUARTERLY = four times per year. '
  'MONTHLY = twelve times per year. '
  'Use this to calculate the payment amount per period: '
  'annual_payment = coupon_rate × face_value; '
  'periodic_payment = annual_payment ÷ payments_per_year.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_coupon_schedule.issue_currency_code IS
  'ISO 4217 three-letter currency code in which the coupon is paid. '
  'Matches the bond''s face currency. '
  'Examples: USD, EUR, GBP, JPY. '
  'Use this to filter or aggregate coupon cash flows by currency.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_coupon_schedule.maturity_date IS
  'Maturity date of the bond making this coupon payment. '
  'Denormalised from the bond record for convenience. '
  'Allows time-to-maturity calculations without joining back to dim_product.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_coupon_schedule.coupon_type_bond IS
  'Coupon type as recorded on the bond master record. '
  'May differ from the coupon_type column if the bond''s coupon structure '
  'changed after issuance (e.g. a step-up bond). '
  'Use coupon_type (from the coupon schedule record) for the period-specific type.';

---
