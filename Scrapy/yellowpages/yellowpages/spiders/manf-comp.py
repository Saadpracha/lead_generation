import scrapy
import re


class ManfCompaniesSpider(scrapy.Spider):
    name = "manf_companies"
    allowed_domains = ["manufacturier.quebec"]
    start_urls = [
        "https://www.manufacturier.quebec/fr/repertoire-des-manufacturiers/organization"
    ]

    def parse(self, response):
        # Get the zoneId and randomId for pagination
        zone_id = response.xpath('//div[@data-zone-alias="Topbar"]/@data-zone-id').get()
        match = re.search(r"randomId=(\d+)", response.url)
        random_id = match.group(1) if match else ""

        # Extract current page number
        current_page = response.xpath('//a[@class="page disabled active"]/text()').get()
        if current_page:
            current_page = int(current_page.strip())
        else:
            current_page = 1

        # Extract all IDs on this page
        ids = response.xpath('//tbody//tr/@id').re(r'view_detailorganization_(\d+)')
        for id_val in ids:
            detail_url = f"https://www.manufacturier.quebec/fr/repertoire-des-manufacturiers/detailorganization/id/{id_val}"
            yield scrapy.Request(
                url=detail_url,
                callback=self.parse_detail
            )

        # Next page logic: Go up to page 187
        if current_page < 187:
            next_page = current_page + 1
            pagination_url = (
                f"https://www.manufacturier.quebec/fr/repertoire-des-manufacturiers/"
                f"pagination/pageNumber/{next_page}?activetab=organization&zoneId={zone_id}&randomId={random_id}"
            )
            yield scrapy.Request(
                url=pagination_url,
                callback=self.parse
            )


    def parse_detail(self, response):
        def clean_text(val):
            if not val:
                return ""
            val = val.strip()
            # Remove both "Address :" and "Address:"
            val = re.sub(r"^Address\s*:?", "", val, flags=re.IGNORECASE).strip()
            return val

        name = clean_text(response.xpath('//h1/text()').get())

        # Extract raw address parts
        address_parts = response.xpath(
            '//div[@class="org-card__inner"]/p[@class="org-card__address"]//text()'
        ).getall()
        address_parts = [clean_text(part) for part in address_parts if clean_text(part)]

        address = city = province = country = postal_code = ""

        if address_parts:
            # Postal code is always last if contains digits
            if re.search(r"\d", address_parts[-1]):
                postal_code = address_parts[-1]
                address_parts = address_parts[:-1]

            # Case 1: First element is "Québec" → no street, no city
            if address_parts and address_parts[0].lower() == "québec":
                province = address_parts[0]
                if len(address_parts) > 1:
                    country = address_parts[1]

            else:
                # If first part starts with a number → street
                if re.match(r"^\d", address_parts[0]):
                    address = address_parts[0]
                else:
                    # not a street (rare case)
                    address = ""

                # Handle city/province/country
                if len(address_parts) > 1:
                    if address_parts[1].lower() == "québec":
                        city = ""
                        province = address_parts[1]
                        if len(address_parts) > 2:
                            country = address_parts[2]
                    else:
                        city = address_parts[1]
                        if len(address_parts) > 2:
                            province = address_parts[2]
                        if len(address_parts) > 3:
                            country = address_parts[3]

        full_address = ", ".join(
            [a for a in [address, city, province, country, postal_code] if a]
        )

        telephone = clean_text(
            response.xpath(
                '//div[@class="org-card__inner"]/p[@class="org-card__phone"]/text()'
            ).get()
        )
        website = response.xpath(
            '//a[@class="org-card__website"]/@href'
        ).get()

        categories = response.xpath(
            '//h2[contains(text(),"Secteurs d\'activités")]/following-sibling::ul/li/text()'
        ).getall()
        categories = [clean_text(cat) for cat in categories]

        yield {
            "name": name,
            "address": address,
            "city": city,
            "province": province,
            "country": country,
            "postal_code": postal_code,
            "full_address": full_address,
            "telephone": telephone,
            "website": website,
            "categories": categories,
        }
