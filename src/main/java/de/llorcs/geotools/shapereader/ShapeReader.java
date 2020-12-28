package de.llorcs.geotools.shapereader;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.NameImpl;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Geotools API:
 * https://docs.geotools.org/latest/userguide/tutorial/quickstart/maven.html
 */
public class ShapeReader {
	
	private static final NameImpl ZH_NAME_IMPL = new NameImpl("NAME_ZH");
	private final FeatureSource<SimpleFeatureType, SimpleFeature> source;
	private final MandarinLocationTransscriber zhToEnglish = MandarinLocationTransscriber.toEnglish();
	private final MandarinLocationTransscriber zhToGerman = MandarinLocationTransscriber.toGerman();

	ShapeReader(File file) throws IOException {
		Map<String, Object> map = new HashMap<>();
        map.put("url", file.toURI().toURL());

        DataStore dataStore = DataStoreFinder.getDataStore(map);
        String typeName = dataStore.getTypeNames()[0];

        source = dataStore.getFeatureSource(typeName);
	}


	public static void main(String[] args) throws IOException {
		if (args.length==0) {
			usageAndExit();
		}

		File file = new File(args[0]);
		
		

		if (!file.exists()) {
			System.out.println("File not found: "+file);
			usageAndExit();
		}
		ShapeReader shapeReader=new ShapeReader(file);
		
		Consumer<SimpleFeature> consumer;
		if (args.length==2 && "-sql".equalsIgnoreCase(args[1])) {
			consumer=feature->shapeReader.toSql(System.out, feature);
		} else {
			consumer=feature->shapeReader.toCsv(System.out, feature);
		}
		
		shapeReader.walkSource(consumer);

	}

	public void walkSource(Consumer<SimpleFeature> featureConsumer) throws IOException {
		Filter filter = Filter.INCLUDE; // ECQL.toFilter("BBOX(THE_GEOM, 10,20,30,40)")
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(filter);
        try (FeatureIterator<SimpleFeature> features = collection.features()) {
            while (features.hasNext()) {
                SimpleFeature feature = features.next();
				featureConsumer.accept(feature);
            }
        }
	}
	
	private void toCsv(PrintStream writer, SimpleFeature feature) {
		writer.print(feature.getID().substring(feature.getID().indexOf('.') + 1));
		writer.print(";" + getTitle(zhToEnglish, feature));
		Point geometryProperty = (Point) feature.getDefaultGeometryProperty().getValue();
		Coordinate coordinate = geometryProperty.getCoordinate();
		writer.println(";" + coordinate.getY() + "," + coordinate.getX());
	}
	
	private void toSql(PrintStream writer, SimpleFeature feature) {
		// beispiel SQL aus https://raw.githubusercontent.com/RailwayStations/Bahnhofsdaten/main/russia/batch-import2.sql
		// INSERT INTO stations (countryCode, id, uicibnr, title, lat, lon) 
		// VALUES ('ru', '5946', NULL, 'Elektrodepo (Электродепо)', 47.1522862, 39.7556311);
		writer.print("INSERT INTO stations");
		writer.print(" (countryCode, id, uicibnr, title, lat, lon)");
		writer.print(" VALUES ");
		writer.print("(");
		writer.print("'cn', "); // country code, fixed cn
		
		writer.print("'");
		writer.print(feature.getID().substring(feature.getID().indexOf('.') + 1)); // id
		writer.print("'");
		writer.print(", ");
		
		writer.print("NULL, "); // uicibnr
		
		writer.print("'");
		writer.print(getTitle(zhToEnglish, feature)); // title
		writer.print("'");
		writer.print(", ");
		
		Point geometryProperty = (Point) feature.getDefaultGeometryProperty().getValue();
		Coordinate coordinate = geometryProperty.getCoordinate();
		writer.print(coordinate.getY()); // lat
		writer.print(", ");
		writer.print(coordinate.getX()); // long
		writer.print(");\n");
	}

	private String getTitle(final MandarinLocationTransscriber transscriber, final SimpleFeature feature) {
		Collection<Property> properties = feature.getProperties();
		Optional<String> optZhName = findZhName(properties);
		return optZhName.map(chinese-> transscriber.translate(chinese) + " (" + chinese + ")").orElse("No Name found.");
	}

	private Optional<String> findZhName(Collection<Property> properties) {
		return properties.stream().filter(prop->ZH_NAME_IMPL.equals(prop.getName())).map(prop->{
			try {
				// re-interpreting the string in UTF-8.
				String rawName=(String)prop.getValue();
				byte[] rawBytes = rawName.getBytes("ISO_8859_1");
				return new String(rawBytes, StandardCharsets.UTF_8);
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}).findFirst();
	}

	private static void usageAndExit() {
		System.out.println("Run with 1 argument with the path to the .shp file.");
		System.exit(-1);
	}

}
