package pl.michalsz.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CollisionData implements WritableComparable<CollisionData> {

    private final Text street;
    private final Text zipCode;
    private final Text personType;
    private final Text damageType;

    public CollisionData() {
        this.street = new Text();
        this.zipCode = new Text();
        this.personType = new Text();
        this.damageType = new Text();
    }

    public CollisionData(String street, String zipCode, String personType, String damageType) {
        this.street = new Text(street);
        this.zipCode = new Text(zipCode);
        this.personType = new Text(personType);
        this.damageType = new Text(damageType);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        street.write(dataOutput);
        zipCode.write(dataOutput);
        personType.write(dataOutput);
        damageType.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        street.readFields(dataInput);
        zipCode.readFields(dataInput);
        personType.readFields(dataInput);
        damageType.readFields(dataInput);
    }

    @Override
    public String toString() {
        return String.format("%s\t%s\t%s\t%s", street, zipCode, personType, damageType);
    }

    @Override
    public int compareTo(CollisionData otherCollisionData) {
        int comparison = street.compareTo(otherCollisionData.street);
        if (comparison != 0) {
            return comparison;
        }

        comparison = zipCode.compareTo(otherCollisionData.zipCode);
        if (comparison != 0) {
            return comparison;
        }

        comparison = personType.compareTo(otherCollisionData.personType);
        if (comparison != 0) {
            return comparison;
        }

        return damageType.compareTo(otherCollisionData.damageType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CollisionData that = (CollisionData) o;
        return Objects.equals(street, that.street)
                && Objects.equals(zipCode, that.zipCode)
                && Objects.equals(personType, that.personType)
                && Objects.equals(damageType, that.damageType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(street, zipCode, personType, damageType);
    }
}
