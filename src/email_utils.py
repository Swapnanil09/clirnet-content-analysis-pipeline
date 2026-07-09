"""
Sends the monthly Content Analysis BI email with the 3 deliverable files
attached, using the exact wording CLIRNET expects — with the month name
resolved dynamically from `config.TARGET_MONTH` instead of being typed by
hand every month.
"""
from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage
from pathlib import Path

from . import config

log = logging.getLogger("pipeline.email")


def build_subject() -> str:
    return f"Content Analysis BI – {config.TARGET_MONTH_LABEL} Data"


def build_body() -> str:
    return (
        "Hi Team,\n\n"
        f"Please find the requested updated data for the Content Analysis BI "
        f"for {config.TARGET_MONTH_LABEL} attached.\n\n"
        "The following data has been shared:\n"
        f"Speciality_month_diff_campaign – {config.MONTH_NAME}\n"
        f"Content_template_analysis – {config.MONTH_NAME}\n"
        "All content data – Current\n\n"
        "This is an automated email.\n\n"
        "Thanks and Regards,\n"
        f"{config.EMAIL_SIGNOFF_NAME}"
    )


def _attachment_mime_type(path: Path) -> tuple[str, str]:
    if path.suffix.lower() == ".csv":
        return "text", "csv"
    if path.suffix.lower() == ".xlsx":
        return "application", "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if path.suffix.lower() == ".zip":
        return "application", "zip"
    return "application", "octet-stream"


def send_report_email(attachments: list[Path] | None = None) -> None:
    attachments = attachments or [
        config.OUT_SPECIALITY_MONTH_DIFF,
        config.OUT_CONTENT_TEMPLATE_ANALYSIS,
        config.OUT_ALL_CONTENT_DATA,
    ]

    missing = [p for p in attachments if not p.exists()]
    if missing:
        raise FileNotFoundError(
            f"Cannot send email — these expected outputs were not generated: {missing}"
        )

    if not config.EMAIL_FROM or not config.EMAIL_PASSWORD:
        raise EnvironmentError("EMAIL_FROM / EMAIL_PASSWORD are not set.")
    if not config.EMAIL_TO:
        raise EnvironmentError("EMAIL_TO is not set — no recipients configured.")

    msg = EmailMessage()
    msg["Subject"] = build_subject()
    msg["From"] = config.EMAIL_FROM
    msg["To"] = ", ".join(config.EMAIL_TO)
    msg.set_content(build_body())

    for path in attachments:
        if path == config.OUT_ALL_CONTENT_DATA:
            zip_path = path.with_suffix(".zip")
            log.info("Zipping %s to %s to reduce attachment size...", path.name, zip_path.name)
            import zipfile
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(path, arcname=path.name)
            path_to_attach = zip_path
        else:
            path_to_attach = path

        maintype, subtype = _attachment_mime_type(path_to_attach)
        msg.add_attachment(path_to_attach.read_bytes(), maintype=maintype, subtype=subtype, filename=path_to_attach.name)

    log.info("Sending report email to %s (%s attachments)", config.EMAIL_TO, len(attachments))
    with smtplib.SMTP(config.EMAIL_SMTP_SERVER, config.EMAIL_SMTP_PORT) as server:
        server.starttls()
        server.login(config.EMAIL_FROM, config.EMAIL_PASSWORD)
        server.send_message(msg)
    log.info("Email sent successfully.")
