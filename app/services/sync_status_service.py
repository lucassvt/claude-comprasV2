"""
Servicio para gestionar el estado de las sincronizaciones
Guarda el historial de ejecuciones de cada proceso del scheduler
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class SyncStatus(str, Enum):
    SUCCESS = "success"
    ERROR = "error"
    RUNNING = "running"
    NEVER = "never"


class SyncType(str, Enum):
    SYNC_STOCK = "sync_stock"
    SYNC_VENTAS = "sync_ventas"
    SYNC_PRODUCTOS = "sync_productos"
    STOCK_IDEAL = "stock_ideal"
    FORECASTING = "forecasting"
    REDISTRIBUCION = "redistribucion"
    REPORTE_DIARIO = "reporte_diario"


# Nombres amigables para cada tipo
SYNC_TYPE_NAMES = {
    SyncType.SYNC_STOCK: "Sincronizacion de Stock",
    SyncType.SYNC_VENTAS: "Sincronizacion de Ventas",
    SyncType.SYNC_PRODUCTOS: "Sincronizacion de Productos",
    SyncType.STOCK_IDEAL: "Calculo Stock Ideal",
    SyncType.FORECASTING: "Forecasting ML",
    SyncType.REDISTRIBUCION: "Analisis Redistribucion",
    SyncType.REPORTE_DIARIO: "Reporte Diario"
}

# Horarios programados
SYNC_SCHEDULES = {
    SyncType.SYNC_STOCK: "Diario 06:00",
    SyncType.SYNC_VENTAS: "Diario 06:30",
    SyncType.SYNC_PRODUCTOS: "Domingos 05:00",
    SyncType.STOCK_IDEAL: "Domingos 06:00",
    SyncType.FORECASTING: "Domingos 07:00",
    SyncType.REDISTRIBUCION: "Domingos 08:00",
    SyncType.REPORTE_DIARIO: "Diario 20:00"
}


class SyncStatusService:
    """
    Servicio para registrar y consultar el estado de las sincronizaciones
    """

    def __init__(self, status_file: str = None):
        """
        Args:
            status_file: Ruta al archivo JSON de estados.
                        Por defecto usa data/sync_status.json
        """
        if status_file:
            self.status_file = Path(status_file)
        else:
            # Crear en la carpeta data del proyecto
            self.status_file = Path(__file__).parent.parent.parent / "data" / "sync_status.json"

        # Crear directorio si no existe
        self.status_file.parent.mkdir(parents=True, exist_ok=True)

        # Inicializar archivo si no existe
        if not self.status_file.exists():
            self._initialize_status_file()

    def _initialize_status_file(self):
        """Crea el archivo de estados con valores iniciales"""
        initial_status = {}
        for sync_type in SyncType:
            initial_status[sync_type.value] = {
                "name": SYNC_TYPE_NAMES[sync_type],
                "schedule": SYNC_SCHEDULES[sync_type],
                "last_run": None,
                "last_status": SyncStatus.NEVER.value,
                "last_message": "Nunca ejecutado",
                "last_duration_seconds": None,
                "run_count": 0,
                "error_count": 0,
                "history": []
            }

        self._save_status(initial_status)

    def _load_status(self) -> Dict:
        """Carga el estado desde el archivo JSON"""
        try:
            with open(self.status_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error cargando sync status: {e}")
            self._initialize_status_file()
            return self._load_status()

    def _save_status(self, status: Dict):
        """Guarda el estado en el archivo JSON"""
        try:
            with open(self.status_file, 'w', encoding='utf-8') as f:
                json.dump(status, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error guardando sync status: {e}")

    def start_sync(self, sync_type: SyncType) -> str:
        """
        Registra el inicio de una sincronizacion

        Returns:
            ID de la ejecucion
        """
        status = self._load_status()

        if sync_type.value not in status:
            self._initialize_status_file()
            status = self._load_status()

        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        status[sync_type.value]["last_run"] = datetime.now().isoformat()
        status[sync_type.value]["last_status"] = SyncStatus.RUNNING.value
        status[sync_type.value]["last_message"] = "En ejecucion..."
        status[sync_type.value]["current_run_id"] = run_id
        status[sync_type.value]["current_start_time"] = datetime.now().isoformat()

        self._save_status(status)
        logger.info(f"Iniciando sync: {sync_type.value} (run_id: {run_id})")

        return run_id

    def end_sync(
        self,
        sync_type: SyncType,
        success: bool = True,
        message: str = None,
        records_processed: int = None
    ):
        """
        Registra el fin de una sincronizacion

        Args:
            sync_type: Tipo de sincronizacion
            success: Si fue exitosa
            message: Mensaje descriptivo
            records_processed: Cantidad de registros procesados
        """
        status = self._load_status()

        if sync_type.value not in status:
            return

        sync_data = status[sync_type.value]

        # Calcular duracion
        duration = None
        if "current_start_time" in sync_data:
            start_time = datetime.fromisoformat(sync_data["current_start_time"])
            duration = (datetime.now() - start_time).total_seconds()

        # Actualizar estado
        sync_data["last_run"] = datetime.now().isoformat()
        sync_data["last_status"] = SyncStatus.SUCCESS.value if success else SyncStatus.ERROR.value
        sync_data["last_duration_seconds"] = duration
        sync_data["run_count"] = sync_data.get("run_count", 0) + 1

        if success:
            if message:
                sync_data["last_message"] = message
            elif records_processed is not None:
                sync_data["last_message"] = f"OK - {records_processed} registros procesados"
            else:
                sync_data["last_message"] = "Completado exitosamente"
        else:
            sync_data["error_count"] = sync_data.get("error_count", 0) + 1
            sync_data["last_message"] = message or "Error en la ejecucion"

        # Agregar al historial (mantener ultimos 10)
        history_entry = {
            "run_id": sync_data.get("current_run_id"),
            "timestamp": datetime.now().isoformat(),
            "status": sync_data["last_status"],
            "message": sync_data["last_message"],
            "duration_seconds": duration,
            "records_processed": records_processed
        }

        if "history" not in sync_data:
            sync_data["history"] = []

        sync_data["history"].insert(0, history_entry)
        sync_data["history"] = sync_data["history"][:10]  # Solo ultimos 10

        # Limpiar campos temporales
        sync_data.pop("current_run_id", None)
        sync_data.pop("current_start_time", None)

        self._save_status(status)

        status_str = "OK" if success else "ERROR"
        logger.info(f"Sync terminado: {sync_type.value} - {status_str} ({duration:.1f}s)")

    def get_all_status(self) -> Dict:
        """
        Obtiene el estado de todas las sincronizaciones

        Returns:
            Dict con el estado de cada tipo de sync
        """
        status = self._load_status()

        # Agregar info adicional para la UI
        result = {}
        for sync_type in SyncType:
            if sync_type.value in status:
                sync_data = status[sync_type.value].copy()

                # Formatear la fecha para mostrar
                if sync_data.get("last_run"):
                    try:
                        last_run = datetime.fromisoformat(sync_data["last_run"])
                        sync_data["last_run_formatted"] = last_run.strftime("%d/%m/%Y %H:%M")

                        # Calcular tiempo transcurrido
                        delta = datetime.now() - last_run
                        if delta.days > 0:
                            sync_data["time_ago"] = f"hace {delta.days} dias"
                        elif delta.seconds >= 3600:
                            hours = delta.seconds // 3600
                            sync_data["time_ago"] = f"hace {hours}h"
                        elif delta.seconds >= 60:
                            minutes = delta.seconds // 60
                            sync_data["time_ago"] = f"hace {minutes}min"
                        else:
                            sync_data["time_ago"] = "hace menos de 1min"
                    except:
                        sync_data["last_run_formatted"] = "N/A"
                        sync_data["time_ago"] = ""
                else:
                    sync_data["last_run_formatted"] = "Nunca"
                    sync_data["time_ago"] = ""

                # Formatear duracion
                if sync_data.get("last_duration_seconds"):
                    duration = sync_data["last_duration_seconds"]
                    if duration >= 60:
                        sync_data["duration_formatted"] = f"{duration/60:.1f} min"
                    else:
                        sync_data["duration_formatted"] = f"{duration:.0f} seg"
                else:
                    sync_data["duration_formatted"] = "-"

                result[sync_type.value] = sync_data
            else:
                # Sync type no inicializado
                result[sync_type.value] = {
                    "name": SYNC_TYPE_NAMES[sync_type],
                    "schedule": SYNC_SCHEDULES[sync_type],
                    "last_run_formatted": "Nunca",
                    "time_ago": "",
                    "last_status": SyncStatus.NEVER.value,
                    "last_message": "Nunca ejecutado",
                    "duration_formatted": "-"
                }

        return result

    def get_sync_status(self, sync_type: SyncType) -> Dict:
        """
        Obtiene el estado de una sincronizacion especifica
        """
        all_status = self.get_all_status()
        return all_status.get(sync_type.value, {})

    def get_history(self, sync_type: SyncType, limit: int = 10) -> List[Dict]:
        """
        Obtiene el historial de ejecuciones de una sync
        """
        status = self._load_status()

        if sync_type.value not in status:
            return []

        history = status[sync_type.value].get("history", [])
        return history[:limit]

    # ==================== Metodos de conveniencia ====================

    def _quick_update(self, sync_type: SyncType, message: str, records_processed: int = None):
        """
        Actualiza estado de una sincronizacion de forma rapida.
        No requiere llamar a start_sync primero.
        """
        status = self._load_status()

        if sync_type.value not in status:
            self._initialize_status_file()
            status = self._load_status()

        sync_data = status[sync_type.value]

        # Actualizar estado
        sync_data["last_run"] = datetime.now().isoformat()
        sync_data["last_status"] = SyncStatus.SUCCESS.value
        sync_data["run_count"] = sync_data.get("run_count", 0) + 1

        if records_processed is not None:
            sync_data["last_message"] = f"{message} - {records_processed} registros"
        else:
            sync_data["last_message"] = message

        # Agregar al historial
        history_entry = {
            "timestamp": datetime.now().isoformat(),
            "status": SyncStatus.SUCCESS.value,
            "message": sync_data["last_message"],
            "records_processed": records_processed
        }

        if "history" not in sync_data:
            sync_data["history"] = []

        sync_data["history"].insert(0, history_entry)
        sync_data["history"] = sync_data["history"][:10]

        self._save_status(status)
        logger.info(f"Sync actualizado: {sync_type.value} - {sync_data['last_message']}")

    def update_sync_stock(self, records_processed: int = None, message: str = None):
        """Actualiza estado de sincronizacion de stock"""
        self._quick_update(
            SyncType.SYNC_STOCK,
            message or "Stock sincronizado correctamente",
            records_processed
        )

    def update_sync_ventas(self, records_processed: int = None, message: str = None):
        """Actualiza estado de sincronizacion de ventas"""
        self._quick_update(
            SyncType.SYNC_VENTAS,
            message or "Ventas sincronizadas correctamente",
            records_processed
        )

    def update_stock_ideal(self, records_processed: int = None, message: str = None):
        """Actualiza estado de calculo de stock ideal/minimo/maximo"""
        self._quick_update(
            SyncType.STOCK_IDEAL,
            message or "Niveles de stock recalculados",
            records_processed
        )

    def update_sync_productos(self, records_processed: int = None, message: str = None):
        """Actualiza estado de sincronizacion de productos"""
        self._quick_update(
            SyncType.SYNC_PRODUCTOS,
            message or "Productos sincronizados correctamente",
            records_processed
        )


# Singleton para uso global
_sync_status_service = None


def get_sync_status_service() -> SyncStatusService:
    """Obtiene la instancia singleton del servicio"""
    global _sync_status_service
    if _sync_status_service is None:
        _sync_status_service = SyncStatusService()
    return _sync_status_service
