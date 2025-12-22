#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cliente robusto para la API de DUX Software ERP
Incluye manejo de rate limiting, reintentos automáticos y paginación

Integrado con la configuración del proyecto (app.core.config)
"""

import os
import sys
import json
import time
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
import logging

# Configurar encoding para Windows
if sys.platform == 'win32':
    import codecs
    sys.stdout.reconfigure(encoding='utf-8')

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RateLimitHandler:
    """Maneja el rate limiting de forma inteligente"""

    def __init__(self,
                 requests_per_minute: int = 12,    # 1 cada 5 segundos = 12 por minuto
                 requests_per_second: float = 0.2, # 1 cada 5 segundos (DUX API limit)
                 backoff_factor: float = 2.0,
                 max_backoff: int = 30):           # Máximo 30 segundos de espera
        """
        Args:
            requests_per_minute: Límite de requests por minuto
            requests_per_second: Límite de requests por segundo
            backoff_factor: Factor de multiplicación para espera exponencial
            max_backoff: Máximo tiempo de espera en segundos
        """
        self.requests_per_minute = requests_per_minute
        self.requests_per_second = requests_per_second
        self.backoff_factor = backoff_factor
        self.max_backoff = max_backoff

        # Control de requests
        self.request_times = []
        self.last_request_time = 0
        self.consecutive_429_errors = 0

    def wait_if_needed(self):
        """Espera si es necesario para respetar rate limits"""
        current_time = time.time()

        # 1. Control de requests por segundo
        time_since_last = current_time - self.last_request_time
        min_interval = 1.0 / self.requests_per_second

        if time_since_last < min_interval:
            wait_time = min_interval - time_since_last
            logger.debug(f"Esperando {wait_time:.2f}s para respetar límite por segundo")
            time.sleep(wait_time)
            current_time = time.time()

        # 2. Control de requests por minuto
        # Remover requests de hace más de 60 segundos
        self.request_times = [t for t in self.request_times if current_time - t < 60]

        if len(self.request_times) >= self.requests_per_minute:
            # Calcular cuánto esperar
            oldest_request = min(self.request_times)
            wait_time = 60 - (current_time - oldest_request)
            if wait_time > 0:
                logger.info(f"Límite por minuto alcanzado. Esperando {wait_time:.2f}s...")
                time.sleep(wait_time)
                current_time = time.time()
                # Limpiar requests antiguos
                self.request_times = [t for t in self.request_times if current_time - t < 60]

        # Registrar esta request
        self.request_times.append(current_time)
        self.last_request_time = current_time

    def handle_429_error(self, retry_after: Optional[int] = None):
        """
        Maneja error 429 con backoff exponencial

        Args:
            retry_after: Segundos sugeridos por el servidor (header Retry-After)
        """
        self.consecutive_429_errors += 1

        if retry_after:
            wait_time = retry_after
        else:
            # Backoff exponencial: 2^n * backoff_factor
            wait_time = min(
                (self.backoff_factor ** self.consecutive_429_errors),
                self.max_backoff
            )

        logger.warning(
            f"Error 429 (intento #{self.consecutive_429_errors}). "
            f"Esperando {wait_time:.2f}s antes de reintentar..."
        )
        time.sleep(wait_time)

    def reset_429_counter(self):
        """Resetea el contador de errores 429 después de una request exitosa"""
        if self.consecutive_429_errors > 0:
            logger.info("Request exitosa, reseteando contador de errores 429")
            self.consecutive_429_errors = 0


class DuxAPIClient:
    """
    Cliente robusto para la API de DUX Software ERP

    Características:
    - Manejo automático de rate limiting
    - Reintentos con backoff exponencial
    - Paginación automática
    - Logging detallado
    - Caché de respuestas (opcional)
    """

    def __init__(self,
                 base_url: str,
                 token: str,
                 empresa_id: int = None,
                 requests_per_minute: int = 12,    # 1 cada 5 segundos = 12 por minuto
                 requests_per_second: float = 0.2, # 1 cada 5 segundos (DUX API limit)
                 max_retries: int = 5,
                 timeout: int = 60):
        """
        Args:
            base_url: URL base de la API
            token: Token de autenticación
            empresa_id: ID de la empresa en DUX (requerido para la mayoría de endpoints)
            requests_per_minute: Límite de requests por minuto
            requests_per_second: Límite de requests por segundo
            max_retries: Máximo número de reintentos en caso de error
            timeout: Timeout para requests en segundos
        """
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.empresa_id = empresa_id
        self.max_retries = max_retries
        self.timeout = timeout

        if not self.base_url or not self.token:
            raise ValueError("Se requiere base_url y token")

        # Configurar sesión HTTP
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': self.token,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'DuxAPIClient/1.0 - La Mascotera'
        })

        # Rate limit handler
        self.rate_limiter = RateLimitHandler(
            requests_per_minute=requests_per_minute,
            requests_per_second=requests_per_second
        )

        # Estadísticas
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'rate_limit_errors': 0,
            'retries': 0
        }

        logger.info(f"DuxAPIClient inicializado - Rate limit: {requests_per_minute}/min, {requests_per_second}/seg")

    def _make_request(self,
                      method: str,
                      endpoint: str,
                      params: Optional[Dict] = None,
                      data: Optional[Dict] = None,
                      retry_count: int = 0) -> requests.Response:
        """
        Realiza una request con manejo de rate limiting y reintentos

        Args:
            method: Método HTTP (GET, POST, etc.)
            endpoint: Endpoint de la API
            params: Parámetros query string
            data: Datos para POST/PUT
            retry_count: Contador interno de reintentos

        Returns:
            Response object de requests

        Raises:
            Exception: Si se excede el máximo de reintentos o hay error no recuperable
        """
        url = f"{self.base_url}{endpoint}"

        # Esperar si es necesario (rate limiting preventivo)
        self.rate_limiter.wait_if_needed()

        try:
            # Realizar request
            logger.debug(f"{method} {endpoint} (intento {retry_count + 1}/{self.max_retries + 1})")

            if method.upper() == 'GET':
                response = self.session.get(url, params=params, timeout=self.timeout)
            elif method.upper() == 'POST':
                response = self.session.post(url, params=params, json=data, timeout=self.timeout)
            elif method.upper() == 'PUT':
                response = self.session.put(url, params=params, json=data, timeout=self.timeout)
            elif method.upper() == 'DELETE':
                response = self.session.delete(url, params=params, timeout=self.timeout)
            else:
                raise ValueError(f"Método HTTP no soportado: {method}")

            self.stats['total_requests'] += 1

            # Manejar error 429 (Rate Limit)
            if response.status_code == 429:
                self.stats['rate_limit_errors'] += 1

                if retry_count >= self.max_retries:
                    self.stats['failed_requests'] += 1
                    raise Exception(
                        f"Máximo de reintentos alcanzado después de {self.max_retries} "
                        f"errores 429 consecutivos"
                    )

                # Obtener Retry-After header si existe
                retry_after = response.headers.get('Retry-After')
                retry_after = int(retry_after) if retry_after else None

                self.rate_limiter.handle_429_error(retry_after)
                self.stats['retries'] += 1

                # Reintentar
                return self._make_request(method, endpoint, params, data, retry_count + 1)

            # Manejar otros errores 5xx (errores del servidor)
            elif 500 <= response.status_code < 600:
                if retry_count >= self.max_retries:
                    self.stats['failed_requests'] += 1
                    response.raise_for_status()

                wait_time = 2 ** retry_count  # Backoff exponencial
                logger.warning(
                    f"Error {response.status_code} del servidor. "
                    f"Reintentando en {wait_time}s..."
                )
                time.sleep(wait_time)
                self.stats['retries'] += 1

                return self._make_request(method, endpoint, params, data, retry_count + 1)

            # Request exitosa
            self.rate_limiter.reset_429_counter()
            self.stats['successful_requests'] += 1

            # Lanzar excepción para otros códigos de error (4xx)
            response.raise_for_status()

            return response

        except requests.exceptions.Timeout:
            if retry_count >= self.max_retries:
                self.stats['failed_requests'] += 1
                raise Exception(f"Timeout después de {self.max_retries} reintentos")

            logger.warning(f"Timeout. Reintentando...")
            time.sleep(2 ** retry_count)
            self.stats['retries'] += 1

            return self._make_request(method, endpoint, params, data, retry_count + 1)

        except requests.exceptions.ConnectionError:
            if retry_count >= self.max_retries:
                self.stats['failed_requests'] += 1
                raise Exception(
                    f"Error de conexión después de {self.max_retries} reintentos. "
                    f"Verifica tu conexión a internet."
                )

            logger.warning(f"Error de conexión. Reintentando en 5s...")
            time.sleep(5)
            self.stats['retries'] += 1

            return self._make_request(method, endpoint, params, data, retry_count + 1)

    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Realiza un GET request

        Args:
            endpoint: Endpoint de la API (ej: '/items')
            params: Parámetros query string

        Returns:
            Respuesta JSON parseada
        """
        response = self._make_request('GET', endpoint, params=params)
        return response.json()

    def post(self, endpoint: str, data: Dict, params: Optional[Dict] = None) -> Dict:
        """
        Realiza un POST request

        Args:
            endpoint: Endpoint de la API
            data: Datos a enviar en el body
            params: Parámetros query string

        Returns:
            Respuesta JSON parseada
        """
        response = self._make_request('POST', endpoint, params=params, data=data)
        return response.json()

    def get_all_pages(self,
                      endpoint: str,
                      params: Optional[Dict] = None,
                      max_pages: Optional[int] = None,
                      page_size: int = 50,  # Máximo permitido por API Dux
                      progress_callback: Optional[Callable] = None) -> List[Dict]:
        """
        Obtiene todos los resultados paginados de un endpoint

        Args:
            endpoint: Endpoint de la API
            params: Parámetros adicionales
            max_pages: Máximo número de páginas a obtener (None = todas)
            page_size: Cantidad de items por página (máximo 50 según API Dux)
            progress_callback: Función a llamar con el progreso (page, total_pages, items_count)

        Returns:
            Lista con todos los items obtenidos
        """
        all_items = []
        current_page = 1
        total_pages = None

        params = params or {}
        # API Dux usa 'limit' (no 'size'), máximo 50
        page_size = min(page_size, 50)
        params['limit'] = page_size

        logger.info(f"Iniciando obtención paginada de {endpoint}")

        while True:
            # API Dux usa 'offset' (no 'page')
            params['offset'] = (current_page - 1) * page_size

            try:
                response = self.get(endpoint, params=params)

                # Detectar estructura de respuesta
                paging_info = None
                if 'results' in response:
                    items = response['results']
                    paging_info = response.get('paging', {})
                elif 'data' in response:
                    items = response['data']
                    paging_info = response.get('paging', {})
                elif isinstance(response, list):
                    items = response
                else:
                    logger.warning(f"Estructura de respuesta no reconocida: {list(response.keys())}")
                    items = []

                if not items:
                    logger.info(f"Página {current_page} sin resultados. Finalizando.")
                    break

                all_items.extend(items)

                # Calcular total de páginas basado en paging info si existe
                if paging_info:
                    actual_page_size = len(items)
                    total_items = paging_info.get('total', 0)
                    if total_items and actual_page_size:
                        total_pages = (total_items + actual_page_size - 1) // actual_page_size
                    else:
                        total_pages = paging_info.get('pages', None)
                else:
                    total_pages = None

                # Callback de progreso
                if progress_callback:
                    progress_callback(current_page, total_pages, len(all_items))

                logger.info(
                    f"Página {current_page}/{total_pages or '?'} - "
                    f"Obtenidos {len(items)} items - "
                    f"Total acumulado: {len(all_items)}"
                )

                # Verificar si hay más páginas
                if max_pages and current_page >= max_pages:
                    logger.info(f"Alcanzado límite de {max_pages} páginas")
                    break

                # Verificar si llegamos al final usando paging info
                has_more_pages = False
                if paging_info:
                    current_page_from_api = paging_info.get('page', current_page)
                    if total_pages and current_page_from_api < total_pages:
                        has_more_pages = True
                    elif paging_info.get('has_next', False):
                        has_more_pages = True
                    elif total_items := paging_info.get('total'):
                        if len(all_items) < total_items:
                            has_more_pages = True

                # Determinar si hay más páginas
                if paging_info:
                    # Si tenemos paging info, usarlo para determinar
                    if not has_more_pages:
                        logger.info("Última página alcanzada (según paging info)")
                        break
                else:
                    # Si NO hay paging info, continuar mientras devuelva items
                    # Solo detenerse si devuelve 0 items (página vacía)
                    if len(items) == 0:
                        logger.info("Última página alcanzada (0 items devueltos)")
                        break
                    # Si devuelve menos items que page_size, es probable que sea la última
                    if len(items) < page_size:
                        logger.info(f"Última página alcanzada ({len(items)} < {page_size} items)")
                        break

                current_page += 1

            except Exception as e:
                logger.error(f"Error obteniendo página {current_page}: {str(e)}")
                raise

        logger.info(f"Finalizado. Total de items obtenidos: {len(all_items)}")
        return all_items

    # ========== Métodos de conveniencia para endpoints específicos ==========

    def get_items(self,
                  page: int = 1,
                  size: int = 20,
                  filters: Optional[Dict] = None) -> Dict:
        """Obtiene productos/items"""
        # API Dux usa offset y limit (no page y size)
        size = min(size, 50)  # Máximo 50
        params = {'offset': (page - 1) * size, 'limit': size}
        if filters:
            params.update(filters)
        return self.get('/items', params=params)

    def get_all_items(self,
                      max_pages: Optional[int] = None,
                      page_size: int = 50,  # Máximo permitido por API Dux
                      filters: Optional[Dict] = None,
                      progress_callback: Optional[Callable] = None) -> List[Dict]:
        """Obtiene TODOS los productos con paginación automática"""
        return self.get_all_pages(
            '/items',
            params=filters,
            max_pages=max_pages,
            page_size=page_size,
            progress_callback=progress_callback
        )

    def get_empresas(self) -> Dict:
        """Obtiene información de empresas"""
        return self.get('/empresas')

    def get_depositos(self) -> Dict:
        """Obtiene depósitos"""
        return self.get('/depositos')

    def get_stock(self,
                  page: int = 1,
                  size: int = 50,
                  filters: Optional[Dict] = None) -> Dict:
        """Obtiene stock"""
        # API Dux usa offset y limit (no page y size)
        size = min(size, 50)  # Máximo 50
        params = {'offset': (page - 1) * size, 'limit': size}
        if filters:
            params.update(filters)
        return self.get('/stock', params=params)

    def get_all_stock(self,
                      max_pages: Optional[int] = None,
                      page_size: int = 50,  # Máximo permitido por API Dux
                      filters: Optional[Dict] = None,
                      progress_callback: Optional[Callable] = None) -> List[Dict]:
        """Obtiene TODO el stock con paginación automática"""
        return self.get_all_pages(
            '/stock',
            params=filters,
            max_pages=max_pages,
            page_size=page_size,
            progress_callback=progress_callback
        )

    def get_ventas(self,
                   page: int = 1,
                   size: int = 50,  # Máximo permitido por API Dux
                   filters: Optional[Dict] = None) -> Dict:
        """Obtiene ventas/facturas"""
        params = {'offset': (page - 1) * size, 'limit': size}
        if filters:
            params.update(filters)
        return self.get('/facturas', params=params)

    def get_all_ventas(self,
                       max_pages: Optional[int] = None,
                       page_size: int = 50,  # Máximo permitido por API Dux
                       filters: Optional[Dict] = None,
                       progress_callback: Optional[Callable] = None) -> List[Dict]:
        """Obtiene todas las ventas con paginación automática"""
        return self.get_all_pages(
            '/facturas',
            params=filters,
            max_pages=max_pages,
            page_size=page_size,
            progress_callback=progress_callback
        )

    def get_stats(self) -> Dict:
        """Retorna estadísticas del cliente"""
        return {
            **self.stats,
            'success_rate': (
                self.stats['successful_requests'] / self.stats['total_requests'] * 100
                if self.stats['total_requests'] > 0 else 0
            )
        }

    def print_stats(self):
        """Imprime estadísticas de uso"""
        stats = self.get_stats()
        print("\n" + "=" * 60)
        print("ESTADÍSTICAS DEL CLIENTE API")
        print("=" * 60)
        print(f"Total de requests:        {stats['total_requests']}")
        print(f"Requests exitosas:        {stats['successful_requests']}")
        print(f"Requests fallidas:        {stats['failed_requests']}")
        print(f"Errores de rate limit:    {stats['rate_limit_errors']}")
        print(f"Reintentos totales:       {stats['retries']}")
        print(f"Tasa de éxito:            {stats['success_rate']:.2f}%")
        print("=" * 60 + "\n")
