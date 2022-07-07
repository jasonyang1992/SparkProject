package Util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class AppConstants {

    public static final String TODAY_DATE = LocalDate.now().format(DateTimeFormatter.ISO_DATE);
}
