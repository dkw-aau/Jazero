package dk.aau.cs.dkwe.edao.jazero.datalake.utilities;

public abstract class Serializer
{
    protected abstract String abstractSerialize();

    public String serialize()
    {
        return abstractSerialize();
    }
}
