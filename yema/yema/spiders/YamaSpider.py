import csv

import json

import scrapy

from ..items import Product, Branch

productsUPC = []
products = []
branches = []


class YemaSpider(scrapy.Spider):

    name = 'startSpider'

    allowed_domains = ['lacomer.com.mx', 'superama.com.mx']

    blocked_request = 0

    start_urls = [
        'https://www.lacomer.com.mx/lacomer-api/api/v1/public/header/inicio?cambioSucc=false&succFmt=200&succId=409',
        'https://www.superama.com.mx/common/GetMenu?storeId=9999/'
    ]

    def closed(self, reason):
        with open("products.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, ['upc_gtin', 'brand', 'name', 'description', 'ingredients', 'package'])
            writer.writeheader()
            for data in products:
                writer.writerow(data)

        with open("branches.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, ['product_id', 'chain', 'branch', 'price', 'category', 'product_url'])
            writer.writeheader()
            for data in branches:
                writer.writerow(data)

        print('===================================       RESUME       ===================================')
        print('number of products ', len(products))
        print('number of branches ', len(branches))
        print('number of blocked request', self.blocked_request)
        print('PD. superama.com.mx return html view with a captcha, so json.loads crashing, that\'s why requests are block ')
        print('==========================================================================================')

    def parse(self, response):
        # lacomer parse
        if 'lacomer.com.mx' in response.url:
            print("Surfing in lacomer.com.mx")
            apartments = self.get_available_apartments(response)
            for apartment in apartments:
                url = 'https://www.lacomer.com.mx/lacomer-api/api/v1/public/pasilloestrucutra/totalarticulospasillo?agruId=' \
                      + str(apartment['id']) \
                      + '&succId=409'

                yield scrapy.Request(url, callback=self.parse_apartment,
                                     meta={'apartment_name': apartment['name'], 'apartment_id': apartment['id']})

        else:
            # superama parse
            print("Surfing in superama.com.mx")
            apartments = self.get_available_apartments_family_line_superama(response)
            if len(apartments) > 0:
                for apartment in apartments:
                    _apartment = apartment['apartment']
                    _family = apartment['family']
                    _line = apartment['line']
                    url = 'https://www.superama.com.mx/buscador/resultadopaginadobeta?busqueda=' \
                          '&departamento=' + _apartment['seoUrlName'] + \
                          '&familia=' + _family['seoUrlName'] + \
                          '&linea=' + _line['seoUrlName'] + \
                          '&storeid=9999' \
                          '&IsLoggedUser=false' \
                          '&start=0' \
                          '&rows=300'

                    yield scrapy.Request(url, callback=self.parse_apartments_family_line_superama)
            else:
                self.print_error(response)

    # lacomercial control

    def get_available_apartments(self, response):

        _apartments = []
        _notFood = [734, 93, 1328, 57, 87, 1215, 733, 53, 732, 78, 100, 949, 1, 1291, 50]

        _content = response.body
        _content = json.loads(_content)
        _content = _content["departamentos"]
        for _apartment in _content:
            _apartment = _apartment.split(':')
            if len(list(filter(lambda x: x == int(_apartment[0]), _notFood))) == 0:
                _apartments.append({
                    'id': _apartment[0],
                    'name': _apartment[1]
                })

        return _apartments

    def parse_apartment(self, response):
        _name = response.meta.get('apartment_name')
        _id = response.meta.get('apartment_id')
        _content = response.body
        _content = json.loads(_content)
        _categories = _content['vecHijos']
        for _category in _categories:
            url = 'https://www.lacomer.com.mx/lacomer-api/api/v1/public/articulopasillo/articulospasillord?filtroSeleccionado=0' \
                  '&idPromocion=0' \
                  '&marca=' \
                  '&noPagina=1' \
                  '&numResultados=20' \
                  '&orden=-1' \
                  '&padreId=' + str(_category['agruId']) + \
                  '&parmInt=1' \
                  '&pasId=' + str(_id) + \
                  '&pasiPort=0' \
                  '&precio=' \
                  '&succId=409'

            yield scrapy.Request(url, callback=self.parse_product, meta={
                'apartment': {'name': _name, 'id': _id},
                'category': {'name': _category['agruDes'], 'id': _category['agruId']},
            })

    def parse_product(self, response):

        _products = response.body
        _products = json.loads(_products)
        _apartmentID = response.meta.get('apartment').get('id')
        _apartmentName = response.meta.get('apartment').get('name')
        _categoryID = response.meta.get('category').get('id')
        _categoryName = response.meta.get('category').get('name')

        for _product in _products['vecArticulo']:
            _urlProduct = 'https://www.lacomer.com.mx/lacomer/#!/detarticulo/' \
                          + str(_product['artEan']) + '/0/' + str(_apartmentID) + '/1///' \
                          + str(_apartmentID) + '?succId=409&succFmt=200'

            _upc = _product['artEan'][:-1]
            _upc = _upc.rjust(13, '0')

            if len(list(filter(lambda x: x == str(_upc), productsUPC))) == 0:
                _newProduct = Product()
                _newProduct['upc_gtin'] = _upc
                _newProduct['brand'] = _product['marDes']
                _newProduct['name'] = _product['artDestv']
                _newProduct['description'] = _product['artDes']
                _newProduct['ingredients'] = None
                _newProduct['package'] = str(_product['artUco']) + ' ' + str(_product['artTun'])
                yield _newProduct
                products.append(_newProduct)
                productsUPC.append(_upc)

            _newBranch = Branch()
            _newBranch['product_id'] = _upc
            _newBranch['chain'] = 'City Market'
            _newBranch['branch'] = 409
            _newBranch['price'] = '$' + str(_product['artPrven'])
            _newBranch['category'] = _apartmentName
            _newBranch['product_url'] = _urlProduct
            yield _newBranch
            branches.append(_newBranch)

    # superama control

    def get_available_apartments_family_line_superama(self, response):

        _apartments = []
        _notFood = ['_vinos_y_licores', 'd_jugos_y_bebidas', 'd_farmacia',
                    'd_lavanderia_hogar_y_mascotas', 'd_higiene_personal_y_belleza',
                    'd_bebes']

        try:
            _content = response.body
            _content = json.loads(_content)
            _content = _content['MenuPrincipal'][0]['Elements']
            for _apartment in _content:
                _nameApartment = _apartment['departmentName']
                if len(list(filter(lambda x: x == str(_nameApartment), _notFood))) == 0:
                    for _family in _apartment['Elements']:
                        for _line in _family['Elements']:
                            _apartments.append({
                                'apartment': _apartment,
                                'family': _family,
                                'line': _line
                            })

        except ValueError:
            self.print_error(response)
            _apartments = []

        return _apartments

    def parse_apartments_family_line_superama(self, response):
        try:
            _products = response.body
            _products = json.loads(_products)
            _products = _products['Products']
            for _product in _products:
                _upc = _product['Upc']
                url = 'https://www.superama.com.mx/consultar/pdp?upc=' + str(_upc) + '&store=9999'
                yield scrapy.Request(url, callback=self.parse_product_superama)
        except ValueError:
            self.print_error(response)

    def parse_product_superama(self, response):
        try:
            _product = response.body
            _product = json.loads(_product)
            _path = _product['UrlProducto']
            _urlProduct = 'https://www.superama.com.mx' + _path
            _upc = _product['Upc'].rjust(13, '0')

            if len(list(filter(lambda x: x == str(_upc), productsUPC))) == 0:
                _newProduct = Product()
                _newProduct['upc_gtin'] = _upc
                _newProduct['brand'] = _product['Brand']
                _newProduct['name'] = _product['Description']
                _newProduct['description'] = _product['Details']
                _newProduct['ingredients'] = _product['Ingredients']
                _newProduct['package'] = None
                yield _newProduct
                products.append(_newProduct)
                productsUPC.append(_upc)

            _newBranch = Branch()
            _newBranch['product_id'] = _upc
            _newBranch['chain'] = 'Superama'
            _newBranch['branch'] = 9999
            _newBranch['price'] = _product['PriceString']
            _newBranch['category'] = _product['SeoDisplayLineaUrlName']
            _newBranch['product_url'] = _urlProduct

            yield _newBranch
            branches.append(_newBranch)

        except ValueError:
            self.print_error(response)

    def print_error(self, response):
        # superama.com.mx return html view with a captcha, so json.loads crash
        self.blocked_request = self.blocked_request + 1
        print('#', self.blocked_request,
              ' : The request was blocked by superama.com.mx, please use a proxy, change IP or take a rest',
              '\n',
              response.url)