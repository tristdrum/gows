package media

import (
	"github.com/h2non/bimg"
	"math"
)

type Size struct {
	Width  uint32
	Height uint32
}

var ThumbnailSize = Size{
	Width:  72,
	Height: 72,
}

var ProfilePictureSize = Size{
	Width:  640,
	Height: 640,
}

var PreviewLinkSize = Size{
	Width:  1024,
	Height: 512,
}

var PreviewLinkBuiltInSize = Size{
	Width:  192,
	Height: 192,
}

// ImageThumbnail generates a thumbnail image from an image.
func ImageThumbnail(image []byte) ([]byte, error) {
	return Resize(image, ThumbnailSize)
}

func ProfilePicture(image []byte) ([]byte, error) {
	return Resize(image, ProfilePictureSize)
}

func ImageAutoThumbnail(image []byte) ([]byte, error) {
	size, err := CurrentSize(image)
	if err != nil {
		return nil, err
	}
	size = SizeForThumbnail(size)
	return Resize(image, size)
}

// Resize generates a thumbnail image from an image.
func Resize(image []byte, size Size) ([]byte, error) {
	img := bimg.NewImage(image)
	options := bimg.Options{
		Width:  int(size.Width),
		Height: int(size.Height),
		Crop:   true,
		Type:   bimg.JPEG,
	}
	resized, err := img.Process(options)
	if err != nil {
		return nil, err
	}
	return resized, nil
}

func CurrentSize(buffer []byte) (Size, error) {
	image := bimg.NewImage(buffer)

	// Get the size
	size, err := image.Size()
	if err != nil {
		return Size{}, err
	}
	s := Size{
		Width:  uint32(size.Width),
		Height: uint32(size.Height),
	}
	return s, nil
}

func SizeWithEdgeLimit(size Size, maxEdge int) Size {
	width := float64(size.Width)
	height := float64(size.Height)

	if width <= float64(maxEdge) && height <= float64(maxEdge) {
		// Already within limits
		return size
	}

	scale := float64(maxEdge) / math.Max(width, height)

	return Size{
		Width:  uint32(width * scale),
		Height: uint32(height * scale),
	}
}

const ImgThumbMaxEdge = 100

// SizeForThumbnail returns the size for a thumbnail image.
// It scales the image to fit within the maximum edge size while maintaining
func SizeForThumbnail(size Size) Size {
	return SizeWithEdgeLimit(size, ImgThumbMaxEdge)
}
