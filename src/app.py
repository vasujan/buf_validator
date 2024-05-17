import streamlit as st
from validators import ValidatorApp

app = ValidatorApp()


st.set_page_config(page_title="BUF Validator", page_icon="assets/favicon.ico", layout="wide")
page_style = """
    <style>
        .appview-container .main .block-container {{
            padding-top: {padding_top}rem;
            padding-bottom: {padding_bottom}rem;
        }}
    </style>""".format(
    padding_top=1, padding_bottom=1
)
st.markdown(page_style, unsafe_allow_html=True)
st.title("Bulk Update Facility (BUF) Validator")


with st.sidebar:
    st.write("### Options")
    with st.form(key="app_form"):
        validator = st.radio(
            "Select the BUF Type:",
            options=app.VALIDATORS,
            index=0,
            # on_change=app.reset
        )
        app.choose(validator)
        app.uploaded_file = st.file_uploader(
            label="Upload the csv file: ",
            type="csv",
            accept_multiple_files=False,
            help="Upload the csv file meant to be sent for BUF update/delete/insert operations."
        )
        submit = st.form_submit_button(label="Submit")

    st.divider()
    st.caption("[Feedback](<mailto:vasu.jain@spglobal.com?subject=BUF Validator Feedback>)")

if submit and app.uploaded_file:
    with st.expander("File preview: ", expanded=False):
        st.dataframe(app.file.df)
    
    st.write("Check results:")
    validation = app.validate(app.file)
    if validation is not None:
        st.dataframe(validation, height=35 * len(validation) + 38)
    
else:
    st.write("Upload file and submit form to continue.")

