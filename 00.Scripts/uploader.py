# %%
import os
import random
import time
import requests
import uuid
import tqdm
import websocket
import json

from dotenv import load_dotenv

load_dotenv()


env_number = {
    "prod": 4380,
    "dev": 4381,
    "qal": 4382
}

proxies = {
    "http": "frp-wsa-01.eu.edfencorp.net:8080",
    "https": "frp-wsa-01.eu.edfencorp.net:8080"
}

# proxies = None

verify = False


def waiting_time():
    return 0.5 + 0.5 * random.random()


class BluepointUploader:
    """
    Parameters
    ----------
    username : str, optional
        Username, by default os.getenv("BLUEPOINT_USERNAME")

    password : str, optional
        Password, by default os.getenv("BLUEPOINT_PASSWORD")

    environment : str, optional
        Environment to connect to, by default "dev"

    proxies : dict, optional
        Proxies to use, by default None

    verify : bool, optional
        Verify SSL, by default False

    Attributes
    ----------
    s : requests.Session
        Session with the logged in user
    cookies : str
        Cookies in the format "cookie1=value1; cookie2=value2"
    
    Methods
    -------
    login(username, password, environment="dev", proxies=None, verify=False)
        Login to Bluepoint

    initiate_ws_session()
        Initiate websocket session

    bulk_import(target, data, decimal_separator=".", date_format="%d/%m/%Y", timezone="")
        Upload data to Bluepoint
    """
    def __init__(
            self,
            username: str = os.getenv("BLUEPOINT_USERNAME"),
            password: str = os.getenv("BLUEPOINT_PASSWORD"),
            environment: str = "dev",
            proxies: dict = None, 
            verify: bool = False) -> None:
        self.proxies = proxies

        self.verify = verify

        self.s = self.login(
            username,
            password,
            environment=environment,
            proxies=proxies,
            verify=verify
            )
        
        self.ws_session = None

    @property
    def cookies(self):
        """
        Returns
        -------
        str
            Cookies in the format "cookie1=value1; cookie2=value2"
        """
        return "; ".join([str(x)+"="+str(y) for x, y in self.s.cookies.items()])

    def login(
            self, 
            username: str, 
            password: str, 
            environment: str = "dev",
            proxies: dict = None, 
            verify: bool = False
            ) -> requests.Session:
        """
        Parameters
        ----------
        username : str
            Username
        password : str
            Password
        environment : str, optional
            Environment to connect to, by default "dev"

        Returns
        -------
        requests.Session
            Session with the logged in user
        """

        s = requests.Session()
        pre_login = s.get(
            "https://www.bluepoint.io/",
            proxies=self.proxies,
            verify=self.verify,
        )

        time.sleep(waiting_time())

        identifier_request = s.post(
            f"https://login.powerfactors.app{pre_login.history[-1].headers['Location']}",
            data={
                "state": pre_login.history[-1].headers['Location'].split("state=")[-1],
                "username": username,
                "js-available": "true",
                "webauthn-available": "true",
                "is-brave": "false",
                "webauthn-platform-available": "false",
                "action": "default",
            },
            proxies=self.proxies,
            verify=self.verify,
        )

        time.sleep(waiting_time())

        password_request = s.post(
            identifier_request.url,
            data={
                "state": identifier_request.history[-1].headers['Location'].split("state=")[-1],
                "username": username,
                "password": password,
                "action": "default",
            },
            proxies=self.proxies,
            verify=self.verify,
        )

        time.sleep(waiting_time())

        auth_request = s.post(
            "https://www.bluepoint.io/api/v2/users/auth0-login",
            json={
                "code": password_request.url.split("code=")[-1],
                "next": "/",
            },
            proxies=self.proxies,
            verify=self.verify,
        )

        time.sleep(waiting_time())

        redirect_request = s.get(
            f"https://www.bluepoint.io{auth_request.json()['redirect_url']}",
            proxies=self.proxies,
            verify=self.verify,
        )

        time.sleep(waiting_time())

        set_env = s.get(
            f"https://www.bluepoint.io/user/client/{env_number[environment]}",
            proxies=self.proxies,
            verify=self.verify,
        )

        time.sleep(waiting_time())

        front_request = s.get(
            "https://www.bluepoint.io/common/api/frontend-env/",
            proxies=self.proxies,
            verify=self.verify,
        )

        return s

    def initiate_ws_session(self) -> websocket.WebSocket:
        # Add 
        """
        This method initiates an HTTP session using the requests library.

        It first creates a new session, then sends a GET request to the specified URL to retrieve the frontend environment data.
        It also handles proxies and SSL verification based on the instance variables self.proxies and self.verify.

        Returns
        -------
        requests.Session
            The initiated HTTP session with the necessary cookies and headers set.
        """
        ws = websocket.WebSocket()
        
        ws.connect(
            f"wss://www.bluepoint.io/websocket/?session_key={self.s.cookies['sessionid']}",
            header={
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive, Upgrade",
                "Pragma": "no-cache",
                "Cache-Control": "no-cache",
            },
            cookie=self.cookies,
            host="www.bluepoint.io",
        )
        return ws

    def bulk_import(
            self,
            target: str,
            data: dict,
            decimal_separator: str = ".",
            date_format: str = "%d/%m/%Y",
            timezone: str = ""
    ) -> tuple:
        """
        Upload on line of data adn return the websocket the success status as well as 
        the websocket response. 

        Parameters
        ----------
        target : str
            Target of the bulk_import, one of the following:
                - "company"
                - "plant"
                - "meter"
        data : dict
            Data to bulk_import
        decimal_separator : str, optional
            Decimal separator, by default "."
        date_format : str, optional
            Date format, by default "%d/%m/%Y"
        timezone : str, optional
            Timezone, by default ""

        Returns
        -------
        tuple
            (success, websocket response)
        """
        should_close = False
        if not self.ws_session:
            self.ws = self.initiate_ws_session()
            should_close = True

        self.ws.send(json.dumps({
            "identification": "43",
            "target": target,
            "action": "import",
            "data": data,
            "meta": {
                "decimal_separator": decimal_separator,
                "date_format": date_format,
                "timezone": timezone
            },
            "process_id": str(uuid.uuid4())
        }))

        result = self.ws.recv()

        if should_close:
            self.ws.close()

        print(result)

        if type(result) == dict:
            return (
                result["success"],
                result
            )

    def bulk_import_array(self,
            target: str,
            data: dict,
            decimal_separator: str = ".",
            date_format: str = "%d/%m/%Y",
            timezone: str = ""
        ) -> list:
        """
        Upload multiple lines of data and return the success status of each line.
        """
        should_close = False
        if not self.ws_session:
            self.ws = self.initiate_ws_session()
            should_close = True

        result = []

        for i, data_line in enumerate(data):
            try:
                result.append(self.bulk_import(
                target,
                data_line,
                decimal_separator=decimal_separator,
                date_format=date_format,
                timezone=timezone
            )[0])
            
            except Exception as e:
                print(f"Error on line {i}: {e}")
                result.append(False)

        if should_close:
            self.ws.close()

        return result
    
    def add_model(self, component_data):
        """
        Parameters
        ----------
        component_data : dict
            {
                csrfmiddlewaretoken: Kb6SOjGH0FqTpanLTh91xGrxOquD1gDJQmvGNoZkNqrX9AWcHYrLCC6SVMS8sorg
                component_type: 20740
                name: Test 2
                manufacturer: 78780
                Diamètre: 1
                Puissance_nominale: 2
                Puissance_installée: 3
                Hauteur_moyeu: 4
            }
        """
        csrf_token_url = "https://www.bluepoint.io/components/component_types/models/new/?next=%2Fcomponents%2Fcomponent_types%2Fmodels%2F"
        r = self.s.get(
            csrf_token_url,
            verify=self.verify,
            proxies=self.proxies
        )

        csrf_token = r.text.split(r'name="csrfmiddlewaretoken" value="')[1].split(r'">')[0]

        data_upload_url = "https://www.bluepoint.io/components/component_types/models/new/"
        r = self.s.post(
            data_upload_url,
            headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": r"https://www.bluepoint.io/components/component_types/models/new/?next=%2Fcomponents%2Fcomponent_types%2Fmodels%2F&mt=section-2"
            },
            data = {
                "csrfmiddlewaretoken": csrf_token,
                **component_data
            },
            verify=self.verify,
            proxies=self.proxies
        )

        return r
    
    def add_multiple_models(self, component_list):
        failed = []
        for i, component in tqdm.tqdm(enumerate(component_list)):
            r = self.add_model(component)
            if len(r.history) == 0 :
                failed.append(component)

            time.sleep(1)

        return failed
    
    def add_document(self, object_id, object_type, document_data):
        """
        Payload example
            {"data":[{
                "name":"Test",
                "url":"https://sharepoint.com/",
                "tags":"",
                "notes":"nites",
                "used_as":"component_model",
                "object_id":"839192"
                }]
            }

        Paramters
        ---------
        object_id: int

        object_type:
            One of : 
                - component

        ducment_data: dict    
        """
        r = self.s.post(
            "https://www.bluepoint.io/api/v2/documents/",
            headers={
                "Content-Type": "application/json",
                "Referer": r"https://www.bluepoint.io/components/component_types/models/view/839192?module=%2Fcomponents%2Fcomponent_types%2Fmodels%2F&mt=section-1",
            },
            json=document_data
        )
        
if __name__ == "__main__":

    bp_bulk_importer = BluepointUploader(verify=verify, proxies=None)

    # bp_bulk_importer.bulk_import(
    #     "company",
    #     data={"company": {"name": "pied pieper"}},
    # )