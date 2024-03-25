import keyring

# 모의계좌 API 앱키와 앱비밀번호를 입력합니다
keyring.set_password('mock_app_key', 'User Name', 'APP Key')
keyring.set_password('mock_app_secret', 'User Name', 'APP Secret')