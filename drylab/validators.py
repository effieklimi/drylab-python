from typing import Optional
from .types import Event
from .schema_registry import validate_schema, UnknownSchemaError
import jsonschema  # Add this import

class EventValidator:
    """A validator for Event objects that maintains validation state."""
    
    def __init__(self, event: Event):
        self.event = event
        self._is_validated: bool = False
        self._validation_error: Optional[Exception] = None
    
    def validate(self) -> bool:
        """Validate the event and store the result.
        
        Returns:
            bool: True if validation succeeded, False otherwise
        """
        try:
            validate_schema(self.event.header.schema_id, self.event.blob)
            self._is_validated = True
            self._validation_error = None
            return True
        except (UnknownSchemaError, jsonschema.exceptions.ValidationError) as e:
            self._validation_error = e
            return False
    
    @property
    def is_valid(self) -> bool:
        """Check if the event has been validated successfully.
        
        Returns:
            bool: True if the event has been validated successfully
        """
        return self._is_validated
    
    @property
    def validation_error(self) -> Optional[Exception]:
        """Get the last validation error if any.
        
        Returns:
            Optional[Exception]: The last validation error or None if validation succeeded
        """
        return self._validation_error