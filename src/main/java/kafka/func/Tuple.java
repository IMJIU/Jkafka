package kafka.func;

/**
 * Created by Administrator on 2017/4/3.
 */
public class Tuple<A, B> {
    public final static Tuple EMPTY = new Tuple();

    public A v1;
    public B v2;

    public boolean isEmpty() {
        if (this == EMPTY) {
            return true;
        }
        return false;
    }

    public Tuple() {
    }

    public Tuple(A v1, B v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public static <A, B> Tuple<A, B> of(A v1, B v2) {
        return new Tuple<A, B>(v1, v2);
    }


    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            Tuple tuple = (Tuple) o;
            if (this.v1 != null) {
                if (!this.v1.equals(tuple.v1)) {
                    return false;
                }
            } else if (tuple.v1 != null) {
                return false;
            }

            if (this.v2 != null) {
                if (this.v2.equals(tuple.v2)) {
                    return true;
                }
            } else if (tuple.v2 == null) {
                return true;
            }

            return false;
        } else {
            return false;
        }
    }

    public int hashCode() {
        int result = this.v1 != null ? this.v1.hashCode() : 0;
        result = 31 * result + (this.v2 != null ? this.v2.hashCode() : 0);
        return result;
    }

    public String toString() {
        return "Tuple [v1=" + this.v1 + ", v2=" + this.v2 + "]";
    }
}
