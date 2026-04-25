import pandas as pd
from io import BytesIO, StringIO
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from datetime import datetime

class ReportGenerator:
    @staticmethod
    def to_csv(data: list) -> str:
        if not data:
            return ""
        df = pd.DataFrame([dict(r) for r in data])
        output = StringIO()
        df.to_csv(output, index=False)
        return output.getvalue()

    @staticmethod
    def to_pdf(title: str, data: list, summary_metrics: dict = None) -> bytes:
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter)
        styles = getSampleStyleSheet()
        elements = []

        # Title
        elements.append(Paragraph(f"<b>{title}</b>", styles['Title']))
        elements.append(Paragraph(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))
        elements.append(Spacer(1, 12))

        # Summary Metrics
        if summary_metrics:
            elements.append(Paragraph("<b>Executive Summary</b>", styles['Heading2']))
            for k, v in summary_metrics.items():
                elements.append(Paragraph(f"{k.replace('_', ' ').title()}: {v}", styles['Normal']))
            elements.append(Spacer(1, 12))

        # Table Data
        if data:
            df = pd.DataFrame([dict(r) for r in data])
            # Limit columns if too many for PDF
            if len(df.columns) > 8:
                df = df.iloc[:, :8]
            
            table_data = [df.columns.to_list()] + df.values.tolist()
            t = Table(table_data)
            t.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            elements.append(t)

        doc.build(elements)
        return buffer.getvalue()
