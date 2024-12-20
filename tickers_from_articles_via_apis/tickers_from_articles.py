"""
This module provides functionality to extract and process company information from news
articles using the Refinitiv API. It includes classes and methods to:

- Extract the content of the <Body> tag from XML-formatted news articles.
- Extract company entities from the XML response of the Refinitiv EIP API.
- Retrieve the IPO date using the PermID API, when the company is publicly traded.
- Handle various exceptions related to XML parsing and API requests.
- Implement a simple rate limiter to control the frequency of API requests.
- Log detailed information about API requests and responses for debugging purposes.
"""

import json
import logging
import os
import sys
import time
import warnings
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import requests
import urllib3
from requests import Response

# Set up logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

# Create a console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)

# Create a logging format
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)

# Add the handler to the logger
log.addHandler(console_handler)


class RefinitivSelectiveTags(Enum):
    """
    Represents the types of entities that can be extracted from an article using
    the Refinitiv EIP API.
    """

    COMPANY = "company"
    PERSON = "person"
    SOCIALTAGS = "socialtags"
    TOPIC = "topic"


@dataclass
class RefinitivEntity:
    """
    Represents an entity extracted from an article using the Refinitiv EIP API.
    """


@dataclass
class CompanyEntity(RefinitivEntity):
    """
    Represents a company entity extracted from an article using the Refinitiv EIP API.

    Attributes:
        org_perm_id (str): The permanent ID of the company.
        org_name (str): The name of the company.
        ticker_symbol (Optional[str]): The stock ticker symbol of the company, if available.
        is_public (bool): Whether the company is publicly traded.
        ipo_date (Optional[str]): The IPO date of the company in YYYY-MM-DD format, if available.
    """

    org_perm_id: str
    org_name: str
    ticker_symbol: Optional[str]
    is_public: bool
    ipo_date: Optional[str] = None


class CompanyExtractionError(Exception):
    """Base exception class for company extraction errors."""


class BodyExtractionError(CompanyExtractionError):
    """Exception raised when body content extraction fails."""


class EntityExtractionError(CompanyExtractionError):
    """Exception raised when company entity extraction fails."""


class CompanyInfoExtractionError(CompanyExtractionError):
    """Exception raised when company info extraction fails."""


def _extract_body_content(xml_content: str) -> str:
    """
    Extract the content of the <Body> tag from the given XML string.

    Args:
        xml_content (str): The XML content containing a <Body> tag.

    Returns:
        str: The content of the <Body> tag.

    Raises:
        BodyExtractionError: If body content extraction fails.
    """
    try:
        root = ET.fromstring(xml_content)
        body = root.find("Body")
        if body is not None and body.text:
            return body.text.strip()
        else:
            raise BodyExtractionError(
                "No <Body> tag found or empty body in the XML content"
            )
    except ET.ParseError as e:
        raise BodyExtractionError(f"Failed to parse XML content: {str(e)}") from e


def _extract_company_entities_from_xml_response(xml_content: str) -> List[CompanyEntity]:
    """
    Extract company entities from the XML response of the Refinitiv EIP API.

    Args:
        xml_content (str): The XML content containing company information.

    Returns:
        List[CompanyEntity]: A list of CompanyEntity objects.

    Raises:
        BodyExtractionError: If required elements are missing or XML parsing fails.
    """
    try:
        # Parse the XML content
        root = ET.fromstring(xml_content)

        # Define the namespaces
        ns = {
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "c": "http://s.opencalais.com/1/pred/",
        }

        company_entities: List[CompanyEntity] = []

        # Find all rdf:Description elements with rdf:type of Company
        for description in root.findall("rdf:Description", ns):
            rdf_type = description.find("rdf:type", ns)
            if rdf_type is not None:
                rdf_resource = rdf_type.attrib.get(
                    "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource"
                )
                if rdf_resource == "http://s.opencalais.com/1/type/er/Company":
                    # Extract name, permid, ticker, and is_public
                    name_element = description.find("c:name", ns)
                    permid_element = description.find("c:permid", ns)
                    ticker_element = description.find("c:ticker", ns)
                    is_public_element = description.find("c:ispublic", ns)

                    # Check if all required elements are present
                    required_elements = {
                        "Name": name_element,
                        "PermID": permid_element,
                        "IsPublic": is_public_element,
                    }

                    if missing_elements := [
                        name for name, elem in required_elements.items() if elem is None
                    ]:
                        raise BodyExtractionError(
                            f"Missing required elements: {', '.join(missing_elements)}"
                        )

                    # Create CompanyEntity object and add to the list
                    company_entity = CompanyEntity(
                        org_perm_id=permid_element.text,
                        org_name=name_element.text,
                        ticker_symbol=(
                            ticker_element.text if ticker_element is not None else None
                        ),
                        is_public=is_public_element.text.lower() == "true",
                        ipo_date=None,  # Will be updated later
                    )
                    company_entities.append(company_entity)

        return company_entities

    except ET.ParseError as e:
        raise BodyExtractionError(f"Failed to parse XML content: {str(e)}") from e
    except Exception as e:
        raise BodyExtractionError(f"Error extracting company entities: {str(e)}") from e


class RateLimiter:
    """
    Implements a simple rate limiter to control the frequency of API requests.
    """

    def __init__(self):
        """Initialize the RateLimiter."""
        self.last_request_time = 0

    def wait(self):
        """
        Wait if necessary to comply with rate limiting.
        """
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < 1:
            sleep_time = 1 - time_since_last_request
            log.debug(f"Rate limit applied. Waiting for {sleep_time:.2f} seconds.")
            time.sleep(sleep_time)
        self.last_request_time = time.time()


class RefinitivAPIClient:
    """
    A client for interacting with Refinitiv APIs to extract company data.

    This client provides methods to identify company entities in article content
    and retrieve detailed company information using the Refinitiv API.
    """

    BASE_URL_INTELLIGENT_TAGGING = "https://api-eit.refinitiv.com/permid/calais"
    BASE_URL_PERMID = "https://permid.org"

    def __init__(self, access_token: str):
        """
        Initialize the RefinitivAPIClient.

        Args:
            access_token (str): The access token for authenticating with the Refinitiv API.
        """
        self.access_token = access_token
        self.session = requests.Session()
        self.session.headers.update({"X-AG-Access-Token": self.access_token})
        self.rate_limiter = RateLimiter()
        log.debug("RefinitivAPIClient initialized")

    def _make_request(
        self,
        method: Callable[..., Response],
        url: str,
        verify: bool = True,
        resp_as_bytes: bool = True,
        **kwargs: Any,
    ) -> Response:
        """
        Make a rate-limited request to the Refinitiv API.

        Args:
            method (Callable[..., Response]): The HTTP method to use for the request.
            url (str): The URL to send the request to.
            verify (bool): Whether to verify SSL certificates. Defaults to True.
            resp_as_bytes (bool): If True, return the response as bytes;
                otherwise, as text. Defaults to True.
            **kwargs: Additional keyword arguments to pass to the request method.

        Returns:
            Response: The response from the API.

        Raises:
            requests.exceptions.HTTPError: If the API request fails.
        """
        self.rate_limiter.wait()  # Wait before making the request

        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore" if not verify else "default",
                category=urllib3.exceptions.InsecureRequestWarning,
            )
            response = method(url, verify=verify, **kwargs)

        self._log_request(
            method=method.__name__,
            url=url,
            response=response,
            resp_as_bytes=resp_as_bytes,
            **kwargs,
        )

        response.raise_for_status()

        return response

    def _log_request(
        self,
        method: str,
        url: str,
        response: Response,
        resp_as_bytes: bool = True,
        **kwargs: Any,
    ) -> None:
        """
        Log request information.

        Args:
            method (str): The HTTP method used.
            url (str): The URL of the request.
            response (Response): The response object.
            log_resp_as_bytes (bool): If True, log response as bytes; otherwise, as text.
                Defaults to True.
            **kwargs: Additional request parameters.
        """

        # Convert request body to JSON if possible
        request_body = kwargs.get("data") or kwargs.get("json")
        if isinstance(request_body, str):
            try:
                request_body = json.loads(request_body)
            except json.JSONDecodeError:
                pass

        # Get the response as bytes or text
        resp_data = response.content if resp_as_bytes else response.text
        resp_info = f"{len(resp_data)} bytes" if resp_as_bytes else resp_data

        log_data: Dict[str, Any] = {
            "method": method,
            "url": url,
            "status_code": response.status_code,
            "request_body": request_body,
            "response": resp_info,
        }

        log.debug("API Request:\n%s", json.dumps(log_data, indent=2))

    def get_entities_from_refinitiv_eip(
        self, article_content: str, selective_tag: RefinitivSelectiveTags
    ) -> List[RefinitivEntity]:
        """
        Identify entities in the given article content using Refinitiv EIP API.

        Args:
            article_content (str): The XML content of the news article.
            selective_tag (RefinitivSelectiveTags): The type of entity to extract.

        Returns:
            List[RefinitivEntity]: A list of RefinitivEntity objects.

        Raises:
            EntityExtractionError: If entity extraction fails.
        """
        try:
            body_content = _extract_body_content(article_content)

            response = self._make_request(
                method=self.session.post,
                url=self.BASE_URL_INTELLIGENT_TAGGING,
                params={
                    "outputFormat": "application/xml",
                    "x-calais-selectiveTags": selective_tag.value,
                },
                data=body_content,
                headers={"Content-Type": "text/raw"},
            )
            response.raise_for_status()

            if selective_tag == RefinitivSelectiveTags.COMPANY:
                return _extract_company_entities_from_xml_response(
                    xml_content=response.text
                )
            else:
                raise NotImplementedError(
                    f"Selective tag {selective_tag} not implemented"
                )

        except (
            requests.RequestException,
            ET.ParseError,
            BodyExtractionError,
        ) as e:
            raise EntityExtractionError(f"Failed to extract entities: {repr(e)}") from e

    def update_company_with_ipo_date(self, company: CompanyEntity) -> None:
        """
        Update the given CompanyEntity with IPO date information from the PermID API.

        Args:
            company (CompanyEntity): The company entity to update.
        """
        try:
            response = self._make_request(
                method=self.session.get,
                url=f"{self.BASE_URL_PERMID}/1-{company.org_perm_id}",
                params={"format": "json-ld"},
                verify=False,
            )

            response.raise_for_status()

            # Parse the response
            data: Dict[str, Any] = response.json()

            # Update the company with the IPO date
            ipo_date = data.get("hasIPODate")
            if ipo_date:
                try:
                    parsed_date = datetime.fromisoformat(ipo_date.replace("Z", "+00:00"))
                    company.ipo_date = parsed_date.strftime("%Y-%m-%d")
                except ValueError:
                    log.warning(
                        f"Failed to parse IPO date: {ipo_date}. Keeping original value."
                    )
                    company.ipo_date = ipo_date
            else:
                log.warning(f"No IPO date found for {company=}.")

        except (requests.RequestException, json.JSONDecodeError) as e:
            log.warning(f"Failed to extract IPO date for {company=}: {repr(e)}")


def get_company_csv_list(article_content: str) -> List[str]:
    """
    Process a news article and extract information about mentioned companies.

    Args:
        article_content (str): The content of the news article. This is
            expected to be in XML format.

    Returns:
        Union[List[str], str]: A list of comma-separated value strings containing
            company information such as PermID, Org name, ticker symbol and IPO date.
    """

    # Initialize the API client
    access_token = os.environ["REFINITIV_ACCESS_TOKEN"]
    api_client = RefinitivAPIClient(access_token)

    # Get company entities from the Refinitiv EIP API
    company_entities: List[CompanyEntity] = api_client.get_entities_from_refinitiv_eip(
        article_content=article_content,
        selective_tag=RefinitivSelectiveTags.COMPANY,
    )

    # Update company data with IPO date if available, sequentially
    for company_entity in company_entities:
        if company_entity.is_public:
            api_client.update_company_with_ipo_date(company=company_entity)

    # Create the list of tuples and sort by PermID
    company_data = sorted(
        [
            (
                company.org_perm_id,
                company.org_name,
                company.ticker_symbol or "NULL",
                company.ipo_date or "NULL",
            )
            for company in company_entities
        ],
        key=lambda x: x[0],
    )

    # Format the output
    return [
        ",".join([org_perm_id, f"'{org_name}'", ticker, ipo_date])
        for org_perm_id, org_name, ticker, ipo_date in company_data
    ]


if __name__ == "__main__":
    fptr = open(os.environ["OUTPUT_PATH"], "w")
    os.environ["REFINITIV_ACCESS_TOKEN"] = "Kf1fmqa3XaGGGsh6wMw5OPlYgsHA1FTz"

    from_article = """<Document><Title>Tech and Finance Industry Update</Title><Body>In recent news, tech giants Apple Inc. and Microsoft Corporation have announced new partnerships with emerging startups. Google's parent company Alphabet Inc. is also making waves with its latest AI developments.In the finance sector, Goldman Sachs Group Inc. and Morgan Stanley are reporting strong quarterly earnings. Meanwhile, the privately-held Koch Industries is expanding its investments in renewable energy.Retail giant Walmart Inc. is facing increased competition from online retailers, while Amazon.com Inc. continues to dominate the e-commerce space. Tesla, Inc. has unveiled its latest electric vehicle model, causing ripples in the automotive industry.In other news, privately-owned Cargill, Incorporated has announced a major agricultural initiative. Visa Inc. and Mastercard Incorporated are rolling out new payment technologies.The startup scene is also buzzing, with privately-held companies like SpaceX and Stripe making headlines for their innovative approaches. Meanwhile, public company Uber Technologies, Inc. is expanding its services globally.Berkshire Hathaway Inc., led by Warren Buffett, has made several high-profile investments across various sectors. Lastly, social media platform Twitter, Inc. is implementing new features to enhance user experience.</Body></Document>"""

    results = get_company_csv_list(from_article)

    # Write results to the output file
    fptr.write("\n".join(results))
    fptr.write("\n")
    fptr.close()
