from datetime import datetime, timedelta
import streamlit as st

def hours_to_hhmm(hours):
    if hours <= 0:
        return "00:00"
    h = int(hours)
    m_float = (hours - h) * 60
    m = int(round(m_float))
    
    if m == 60:
        h += 1
        m = 0
        
    return f"{h:02d}:{m:02d}"

def copy_to_clipboard_js(text_to_copy, button_id):
    js = f"""
    <script>
    (function() {{
        const textToCopy = {repr(text_to_copy)};
        const allButtons = Array.from(window.parent.document.querySelectorAll('button[data-testid="stButton"]'));
        const button = allButtons.find(btn => btn.innerText && btn.innerText.includes("{button_id}"));
        
        const textArea = document.createElement("textarea");
        textArea.value = textToCopy;
        textArea.style.position = "fixed";
        textArea.style.left = "-9999px";
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();
        try {{
            const successful = document.execCommand('copy');
            if (successful) {{
                if (button) {{
                    const originalText = button.innerText;
                    button.innerText = 'Copied!';
                    setTimeout(() => {{
                        if (button) {{
                            button.innerText = originalText;
                        }}
                    }}, 2000);
                }}
            }} else {{
                if (button) {{
                    button.innerText = 'Copy Failed';
                }}
            }}
        }} catch (err) {{
            console.error('Fallback: Oops, unable to copy', err);
            if (button) {{
                button.innerText = 'Copy Failed';
            }}
        }}
        document.body.removeChild(textArea);
    }})();
    </script>
    """
    return js