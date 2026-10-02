# Rules

How Kafka's Hoard turns dates into deadlines. The code is in `kafka_hoard/extract/rules.py` (pure functions), `kafka_hoard/bizdays.py` (calendar) and `kafka_hoard/extract/deadlines.py` (which deadline each document produces). Every deadline stores its basis (the rule, the base date, the days counted) and the quote and page it comes from; «why this date?» in the UI and the `deadline_explain` tool show them.

These are the rules of an assistant, not legal advice. For anything important check the original notice.

## Administrative periods — Ley 39/2015, art. 30

Applies to letters from the administration (tax office, town hall, social security, traffic authority) that give a period to respond, allege, appeal or pay.

- Periods in **days** are **working days** (días hábiles) unless the text says «naturales». Saturdays, Sundays and holidays do not count.
- Counting starts **the day after** the notification (or, when the notification date is not in the text, the day the mail arrived, else the issue date of the document; the basis says which).
- Periods in **months or years** run from date to date: the same day number n months later, or the last day of the month when there is no such day (31 January + 1 month = 28 or 29 February).
- When the last day is not a working day, the period ends on the **next working day**.

Words that set the unit: «hábiles», «laborables» (working days), «naturales», «de calendario» (natural days). A period with a number and no word is read as working days when it is in days.

Example: notified Monday 5 October 2026, «10 días hábiles» → 6, 7, 8 and 9 October count; 10 and 11 October are a weekend and 12 October is a national holiday, 13, 14, 15, 16, 19 and 20 October count → the deadline is **20 October 2026**.

## Traffic fines — dgt.es

A fine can be paid with a **50 % reduction** and, in that case, without allegations, within **20 natural days** from the notification; the same 20 natural days apply to make allegations. Kafka creates a `fine_discount` deadline with the discounted amount when the notice gives one. When the notice states its own period for the reduction (for example «15 días hábiles»), that period is used instead and counted as the text says. Periods the notice gives for allegations or appeals become `appeal` deadlines counted under Ley 39/2015 (above). A last day that falls on a weekend or holiday moves to the next working day. The base is the notification date when written; otherwise the arrival date of the mail.

## Insurance renewal — Ley 50/1980, art. 22 (as amended by Ley 20/2015)

The policyholder opposes the renewal by written notice **at least one month before** the end of the current period; the insurer must do so **two months** before. Kafka creates:

- a `renewal` deadline on the renewal date written in the policy («fecha de renovación», «vencimiento», «efecto hasta»), or one year after the effective date when only that is given (marked as an estimate);
- a `cancel_by` deadline one month before it (the same day number one month earlier, or the last day of the month), explained as the policyholder's notice.

Pure payments (the premium receipt) are a separate `payment` deadline.

## Legal warranty — Real Decreto Legislativo 7/2021 (consumer law)

New goods have a legal guarantee of **3 years** from delivery. Kafka creates a `warranty_end` deadline for purchases (receipts, warranty certificates, shop invoices and any document with an item) counting from the delivery date, else the purchase date, else the issue date. The number of years is a setting (`warranty.years`, 1–10); a period written in the document («garantía de 2 años») wins. Utilities, telecom, insurers, banks and public bodies get no warranty deadline.

## Contracts and subscriptions

- **Commitment** («permanencia de 12 meses»): `permanence_end` = effective date + n months, calendar arithmetic, no working-day shift.
- **Renewal**: the renewal date written in a contract or subscription becomes a `renewal` deadline (yearly when the text says annual). A monthly subscription becomes a recurring monthly `payment` instead.
- **Payments**: the due date of an invoice or bill, or the date of the next charge of a subscription.

## Instruction manuals

A document of kind `manual` (given when filing it, chosen in Detalle, or recognised by its wording) is stored and searched like the rest but never produces a deadline, and it does not need a date to be complete.

## Documents with an expiry

Identity documents (DNI, passport, driving licence, health card, residence card) produce an `expiry` deadline on the date of validity, with 90, 30 and 7 days of notice by default. Other documents get an `expiry` deadline only when the expiry date is stated explicitly (a label such as «válido hasta» or «caducidad»). A vehicle inspection report produces an `itv` deadline on «próxima inspección» (or the expiry date).

## Calendar

National holidays: 1 January, 6 January, Good Friday, 1 May, 15 August, 12 October, 1 November, 6 December, 8 December, 25 December. Regional additions for the settings `calendar.region`: ES-MD (2 May, Holy Thursday), ES-CT (Easter Monday, 24 June, 11 September, 26 December), ES-AN (28 February, Holy Thursday), ES-VC (19 March, 9 October, Holy Thursday), ES-GA (17 May, 25 July, Holy Thursday), ES-PV (25 July, Holy Thursday). Local holidays go in `calendar.extra_holidays` (comma-separated `YYYY-MM-DD`). Holidays that fall on a Sunday and are moved are not modelled; add the Monday by hand.

## When a date becomes a deadline

- A deadline is created only when the date is today or later, except administrative ones (fine, official notice, tax), which are kept up to 60 days after their end so a recent letter still shows what you may have missed.
- Dates from a document that is old history (a mail older than two days on the first scan, an old file) create their deadlines already as done.
- A date without a role (for example a bare date in a table) never becomes a deadline; it stays as a fact with its evidence.
- Your edits always win: a deadline you changed (date, title, reminders) is not replaced when the document is read again, and a field you corrected on the document is used by every rule.

## Deadlines from other apps

Another app of the family (HomeHoard sends the next date of each maintenance task) can keep its deadlines here with `deadline_add` plus `source` and `external_key`. The pair is unique: sending it again never duplicates.

- The app sends the date, the title, the reminders, the basis (for example «Mantenimiento por empresa habilitada al menos cada 2 años (RITE, RD 1027/2007, IT 3.3)») and a short rule name. «Why this date?» shows that basis and rule; Kafka does not compute or check them.
- A **new date** from the app is a new occurrence: the deadline moves to it, reopens and its reminders start again.
- **Your edits win for the current occurrence**: a title, reminders or notes you changed here are kept; a date you moved stays until the app sends a different date; a deadline you marked done stays done until the next occurrence; a deadline you dismissed stays dismissed.
- `deadline_update_by_key` lets the app close (done, dismissed), reopen or reschedule its deadline. It cannot reopen one you dismissed.
- Reminders follow the same lead days and channels as every other deadline and link back to the app (`url`).

## Reminders

Default days of notice per kind (setting `remind.<kind>`, comma-separated):

| Kind | Days before |
|---|---|
| `payment` | 3, 0 |
| `renewal` | 45, 30, 7 |
| `cancel_by` | 14, 3, 0 |
| `warranty_end` | 60, 14 |
| `permanence_end` | 30, 0 |
| `appeal` | 5, 2, 0 |
| `fine_discount` | 5, 2, 0 |
| `expiry` | 90, 30, 7 |
| `itv` | 30, 7 |
| `tax` | 15, 5, 1 |
| `custom` | 7, 1 |

Once the day of a notice has come, one notification is sent for the deadline (the most urgent lead not yet notified), once. A deadline that passes without being closed gets one «overdue» notification if it is at most 14 days late. Severity is high when overdue, due today, or an appeal, fine discount or cancel-by within three days; medium within a week; low otherwise. Nothing is sent between `notify.night_from` and `notify.night_to` (23–7 by default) except high-severity ones when `notify.night_high` is on; the rest waits for the morning. Each channel has an on switch and a minimum severity.

## Price changes

Insurance, subscriptions and (with `prices.bills`) utility bills of the same issuer and reference form a series. When the amount of a new document differs from the previous one by `prices.alert_pct` (5 % by default) or more, a `price_change` notification is sent with the old and new amount.

## Family links

- Ledger link: an invoice or receipt with an amount and a date is linked to a Ledger movement only when exactly one candidate (amount equal, date within 5 days) scores 0.8 or more; otherwise the candidates are listed and the user picks. A manual choice always links.
- Agenda priority: overdue or last-day deadlines are urgent when missing them costs money (appeal, fine discount, cancellation window, tax, payment), high otherwise; up to 3 days left: high for those kinds, normal for the rest; up to 14 days: normal and low; later: low.
- Meeting minutes: only action items with a date, owned by the user (`minutes.me`, or «yo») or by nobody, become deadlines; items of others and undated ones are reported, not created.
- Tax return folder: fiscal year is the calendar year; the folder is new each time («Renta 2025 (2)»), originals are copied, never moved. Kafka lists the usual certificates that are missing; it does not decide what is deductible.
